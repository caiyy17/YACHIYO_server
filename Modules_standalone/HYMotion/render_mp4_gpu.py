"""
Render HY-Motion output as MP4 with PyTorch3D GPU-accelerated mesh rendering.
Checkerboard ground (fixed in world space), camera follows root, skeleton overlay.

Usage:
    python custom/render_mp4_gpu.py \
        --npz_dir output/inspect_std_batch --all
    python custom/render_mp4_gpu.py \
        --npz_dir output/inspect_std_batch --indices 1 3 5
"""
import argparse
import glob
import json
import os
import os.path as osp
import subprocess
import sys
import tempfile

# Add project root to path so 'from hymotion...' works from custom/
PROJECT_ROOT = osp.dirname(osp.dirname(osp.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

import numpy as np
import torch
from PIL import Image, ImageDraw

from pytorch3d.structures import Meshes
from pytorch3d.renderer import (
    look_at_view_transform,
    FoVPerspectiveCameras,
    PointLights,
    RasterizationSettings,
    MeshRenderer,
    MeshRasterizer,
    SoftPhongShader,
    TexturesVertex,
)
from hymotion.utils.geometry import axis_angle_to_matrix, rotation_matrix_to_rot6d

SKELETON_LINKS = [
    (0, 1), (0, 2), (0, 3),
    (1, 4), (2, 5), (3, 6),
    (4, 7), (5, 8), (6, 9),
    (7, 10), (8, 11), (9, 12), (9, 13), (9, 14),
    (12, 15), (13, 16), (14, 17),
    (16, 18), (17, 19),
    (18, 20), (19, 21),
]


def create_checkerboard_ground(traj_min, traj_max, device,
                                grid_step=0.5, margin=3.0):
    """
    Create a checkerboard ground plane mesh at y=0, fixed in world space.
    Covers the bounding box of the full trajectory plus margin.
    Grid positions are always at multiples of grid_step from world origin,
    so the pattern never shifts between frames.
    """
    x_lo = np.floor((traj_min[0] - margin) / grid_step) * grid_step
    x_hi = np.ceil((traj_max[0] + margin) / grid_step) * grid_step
    z_lo = np.floor((traj_min[2] - margin) / grid_step) * grid_step
    z_hi = np.ceil((traj_max[2] + margin) / grid_step) * grid_step

    all_verts = []
    all_faces = []
    all_colors = []
    vi = 0

    c_dark = [0.16, 0.16, 0.18]
    c_light = [0.22, 0.22, 0.24]

    for xv in np.arange(x_lo, x_hi, grid_step):
        xi = int(np.round(xv / grid_step))
        for zv in np.arange(z_lo, z_hi, grid_step):
            zi = int(np.round(zv / grid_step))
            c = c_dark if (xi + zi) % 2 == 0 else c_light

            all_verts.extend([
                [xv, 0.0, zv],
                [xv + grid_step, 0.0, zv],
                [xv + grid_step, 0.0, zv + grid_step],
                [xv, 0.0, zv + grid_step],
            ])
            all_faces.extend([
                [vi, vi + 1, vi + 2],
                [vi, vi + 2, vi + 3],
            ])
            all_colors.extend([c, c, c, c])
            vi += 4

    verts = torch.tensor(all_verts, dtype=torch.float32, device=device)
    faces = torch.tensor(all_faces, dtype=torch.int64, device=device)
    colors = torch.tensor(all_colors, dtype=torch.float32, device=device)

    n_tiles = len(all_faces) // 2
    print(f"  Ground grid: {n_tiles} tiles "
          f"([{x_lo:.1f},{x_hi:.1f}] x [{z_lo:.1f},{z_hi:.1f}])")
    return verts, faces, colors


def project_joints_to_screen(joints_3d, cameras, image_size):
    """
    Project 3D joints to 2D screen coordinates using PyTorch3D camera.

    Args:
        joints_3d: (J, 3) tensor on device
        cameras: FoVPerspectiveCameras
        image_size: int

    Returns:
        (J, 2) numpy array of (x, y) pixel coordinates
    """
    pts = joints_3d.unsqueeze(0)  # (1, J, 3)
    # transform_points_screen returns (1, J, 3): x=[0,W], y=[0,H], z=depth
    screen = cameras.transform_points_screen(
        pts, image_size=(image_size, image_size)
    )
    return screen[0, :, :2].cpu().numpy()  # (J, 2)


def render_motion_to_mp4(vertices_seq, faces, keypoints3d, output_path,
                          title="", fps=30, device=None, image_size=720):
    """
    Render mesh animation to MP4 using PyTorch3D.

    Args:
        vertices_seq: (T, V, 3) numpy array — mesh vertices per frame
        faces: (F, 3) numpy array — face indices (shared across frames)
        keypoints3d: (T, J, 3) numpy array — joint positions per frame
                     (must include translation already)
        output_path: output MP4 path
        title: prompt text overlay
        fps: frame rate
        device: torch device
        image_size: output video resolution (square)
    """
    if device is None:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    T, V, _ = vertices_seq.shape
    print(f"  Rendering {T} frames @ {fps}fps ({T/fps:.1f}s) on {device} ...")

    # Shared face tensor
    faces_tensor = torch.tensor(faces, dtype=torch.int64, device=device)

    # Create renderer (shared across frames)
    raster_settings = RasterizationSettings(
        image_size=image_size,
        blur_radius=0.0,
        faces_per_pixel=1,
        bin_size=0,  # naive rasterization — avoids bin overflow with large meshes
    )
    renderer = MeshRenderer(
        rasterizer=MeshRasterizer(raster_settings=raster_settings),
        shader=SoftPhongShader(device=device),
    )

    # Create ground grid once — covers entire trajectory
    traj_min = vertices_seq.min(axis=(0, 1))  # (3,)
    traj_max = vertices_seq.max(axis=(0, 1))  # (3,)
    g_verts, g_faces, g_colors = create_checkerboard_ground(
        traj_min, traj_max, device, grid_step=0.5, margin=3.0
    )

    # Precompute body vertex colors
    body_color = torch.tensor([[0.76, 0.60, 0.42]], dtype=torch.float32, device=device)
    body_colors = body_color.expand(V, -1)  # (V, 3)

    # Precompute combined face indices and color (ground offset fixed)
    combined_faces = torch.cat([faces_tensor, g_faces + V], dim=0)
    combined_colors = torch.cat([body_colors, g_colors], dim=0)

    with tempfile.TemporaryDirectory() as tmpdir:
        for t in range(T):
            verts_t = torch.tensor(
                vertices_seq[t], dtype=torch.float32, device=device
            )
            pelvis = keypoints3d[t, 0]  # (3,) numpy

            # Camera: follow pelvis
            at_point = (
                float(pelvis[0]),
                float(pelvis[1]) + 0.3,
                float(pelvis[2]),
            )
            R, Tv = look_at_view_transform(
                dist=3.5, elev=15.0, azim=-30.0, at=(at_point,),
            )
            cameras = FoVPerspectiveCameras(
                device=device, R=R, T=Tv, fov=45.0
            )

            lights = PointLights(
                device=device,
                location=((
                    float(pelvis[0]) + 2.0,
                    float(pelvis[1]) + 3.0,
                    float(pelvis[2]) + 2.0,
                ),),
                ambient_color=((0.4, 0.4, 0.4),),
                diffuse_color=((0.7, 0.65, 0.6),),
                specular_color=((0.15, 0.15, 0.15),),
            )

            # Combine body + ground vertices
            all_verts = torch.cat([verts_t, g_verts], dim=0)

            textures = TexturesVertex(verts_features=[combined_colors])
            meshes = Meshes(
                verts=[all_verts], faces=[combined_faces], textures=textures
            )

            # Render
            images = renderer(meshes, cameras=cameras, lights=lights)
            image = images[0, ..., :3].cpu().numpy()
            image = (image * 255).clip(0, 255).astype(np.uint8)

            # Overlay skeleton in 2D
            joints = keypoints3d[t, :22]  # (22, 3)
            joints_t = torch.tensor(
                joints, dtype=torch.float32, device=device
            )
            screen_pts = project_joints_to_screen(
                joints_t, cameras, image_size
            )

            img = Image.fromarray(image)
            draw = ImageDraw.Draw(img)

            # Skeleton lines
            for i, j in SKELETON_LINKS:
                x1, y1 = screen_pts[i]
                x2, y2 = screen_pts[j]
                # Only draw if within image bounds (avoid stray lines)
                if (0 <= x1 <= image_size and 0 <= y1 <= image_size and
                        0 <= x2 <= image_size and 0 <= y2 <= image_size):
                    draw.line(
                        [(x1, y1), (x2, y2)],
                        fill=(34, 102, 204), width=2,
                    )

            # Joint dots
            for pt in screen_pts:
                x, y = pt
                if 0 <= x <= image_size and 0 <= y <= image_size:
                    draw.ellipse(
                        [(x - 3, y - 3), (x + 3, y + 3)],
                        fill=(34, 102, 204),
                    )

            # Text overlay
            if title:
                short = title if len(title) <= 80 else title[:77] + "..."
                draw.text((15, 12), short, fill=(230, 230, 230))
            draw.text(
                (image_size - 140, image_size - 25),
                f"Frame {t+1}/{T}",
                fill=(180, 180, 180),
            )

            frame_path = os.path.join(tmpdir, f"frame_{t:06d}.png")
            img.save(frame_path)

            if (t + 1) % 30 == 0 or t == T - 1:
                print(f"    Frame {t+1}/{T}")

        # Encode with ffmpeg — pad filter ensures even dimensions
        cmd = [
            "ffmpeg", "-y", "-loglevel", "warning",
            "-framerate", str(fps),
            "-i", os.path.join(tmpdir, "frame_%06d.png"),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-crf", "18",
            "-preset", "medium",
            "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ffmpeg error: {result.stderr}")
            return False

    file_size = os.path.getsize(output_path) / 1024
    print(f"  Saved: {output_path} ({file_size:.0f} KB)")
    return True


# ---------------------------------------------------------------------------
# Module-level body model cache
# ---------------------------------------------------------------------------
_body_model = None
_body_faces = None


def _get_body_model():
    """Lazy-load and cache WoodenMesh body model."""
    global _body_model, _body_faces
    if _body_model is None:
        from hymotion.pipeline.body_model import WoodenMesh
        model_path = "scripts/gradio/static/assets/dump_wooden"
        print("Loading body model (first call)...")
        _body_model = WoodenMesh(model_path).to(torch.device("cpu"))
        _body_faces = np.array(_body_model.faces)
        print(f"  Mesh: {len(_body_faces)} faces")
    return _body_model, _body_faces


def compute_ground_offset(poses, trans):
    """Run body model forward pass and return min vertex y.

    Args:
        poses: (T, 156) axis-angle
        trans: (T, 3) translation
    Returns:
        float — the minimum y across all vertices and frames.
    """
    body_model, _ = _get_body_model()
    T_frames = poses.shape[0]

    poses_t = torch.tensor(poses, dtype=torch.float32)
    trans_t = torch.tensor(trans, dtype=torch.float32)
    aa = poses_t.reshape(T_frames, 52, 3)
    rot_mats = axis_angle_to_matrix(aa.reshape(-1, 3)).reshape(T_frames, 52, 3, 3)
    rot6d = rotation_matrix_to_rot6d(rot_mats.reshape(-1, 3, 3)).reshape(T_frames, 52, 6)

    with torch.no_grad():
        out = body_model.forward({"rot6d": rot6d, "trans": trans_t})
        vertices = out["vertices"].numpy()

    return float(vertices[:, :, 1].min())


def render_npz_to_mp4(npz_path, output_path=None, title="",
                       fps=30, device=None, image_size=512):
    """Load SMPLH npz -> GPU render -> return MP4 path.

    The npz must contain poses (T, 156) and trans (T, 3).
    """
    body_model, faces = _get_body_model()

    npz = np.load(npz_path, allow_pickle=True)
    poses = npz["poses"]    # (T, 156)
    trans = npz["trans"]     # (T, 3)
    T_frames = poses.shape[0]
    print(f"  Loaded: {npz_path}  ({T_frames} frames, {T_frames/30:.1f}s)")

    # axis-angle -> rot6d -> forward pass
    poses_t = torch.tensor(poses, dtype=torch.float32)
    trans_t = torch.tensor(trans, dtype=torch.float32)
    aa = poses_t.reshape(T_frames, 52, 3)
    rot_mats = axis_angle_to_matrix(aa.reshape(-1, 3)).reshape(T_frames, 52, 3, 3)
    rot6d = rotation_matrix_to_rot6d(rot_mats.reshape(-1, 3, 3)).reshape(T_frames, 52, 6)

    with torch.no_grad():
        out = body_model.forward({"rot6d": rot6d, "trans": trans_t})
        vertices = out["vertices"].numpy()
        keypoints3d = out["keypoints3d"].numpy()
        keypoints3d = keypoints3d + trans[:, None, :]

    if output_path is None:
        output_path = npz_path.replace(".npz", ".mp4")

    if device is None:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    render_motion_to_mp4(vertices, faces, keypoints3d, output_path,
                         title=title, fps=fps, device=device,
                         image_size=image_size)
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Render HY-Motion outputs as MP4 (PyTorch3D GPU)"
    )
    parser.add_argument("--npz_dir", type=str, required=True,
                        help="Directory with *_smplh_motion.npz files")
    parser.add_argument("--indices", type=int, nargs="+", default=None,
                        help="Specific indices to render (e.g. 1 3 5)")
    parser.add_argument("--all", action="store_true",
                        help="Render all motions in directory")
    parser.add_argument("--output_dir", type=str, default=None)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--image_size", type=int, default=720)
    args = parser.parse_args()

    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    output_dir = args.output_dir or os.path.join(args.npz_dir, "videos_gpu")
    os.makedirs(output_dir, exist_ok=True)

    # Find motions
    if args.indices:
        base_names = [f"{idx:03d}" for idx in args.indices]
    elif args.all:
        meta_files = sorted(
            glob.glob(os.path.join(args.npz_dir, "*_meta.json"))
        )
        base_names = [
            os.path.basename(f).replace("_meta.json", "") for f in meta_files
        ]
    else:
        meta_files = sorted(
            glob.glob(os.path.join(args.npz_dir, "*_meta.json"))
        )[:3]
        base_names = [
            os.path.basename(f).replace("_meta.json", "") for f in meta_files
        ]

    if not base_names:
        print("No motions found!")
        return

    print(f"\nRendering {len(base_names)} motion(s) to MP4 (PyTorch3D GPU)...")

    for base_name in base_names:
        # Support both batch_inspect.py and local_infer.py naming
        smplh_path = os.path.join(args.npz_dir, f"{base_name}_smplh_motion.npz")
        legacy_path = os.path.join(args.npz_dir, f"{base_name}_000.npz")
        meta_path = os.path.join(args.npz_dir, f"{base_name}_meta.json")

        if os.path.exists(smplh_path):
            npz_path = smplh_path
        elif os.path.exists(legacy_path):
            npz_path = legacy_path
        else:
            print(f"  Skip {base_name}: no SMPLH NPZ found")
            continue

        title = ""
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
                title = meta.get("prompt", meta.get("text", ""))

        print(f"\n{'='*60}")
        print(f"[{base_name}] {title}")
        print(f"{'='*60}")

        out_path = os.path.join(output_dir, f"{base_name}.mp4")
        render_npz_to_mp4(
            npz_path, output_path=out_path, title=title,
            fps=args.fps, device=device, image_size=args.image_size,
        )

    print(f"\nDone! Videos saved to: {output_dir}")


if __name__ == "__main__":
    main()
