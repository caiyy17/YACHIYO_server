"""
SMPL-H -> engine-native humanoid motion format.

Converts SMPL-H pose params (axis-angle, local-to-parent) + root translation into the
humanoid format consumed by HumanoidMotionPlayer on the Unity client:
  - per-bone WORLD-space deformation quaternions keyed by HumanBodyBones name
  - root motion as per-frame deltas (root_xz / root_vel_y / root_vel_yaw)
  - body offset (~Unity bodyPosition) carried on the hips (hips_pos)

The math mirrors the Unity client's conversion exactly:
  axis-angle -> quaternion, forward kinematics with Unity's Hamilton product,
  X-mirror (SMPL right-handed -> Unity left-handed). Verified frame-by-frame against
  the client converter (0 deg / 0 m difference).

Pure stdlib (no framework deps); accepts numpy arrays or plain lists.
"""
import math

NUM_JOINTS = 52

# SMPL-H parent index per joint (-1 = root).
PARENTS = [-1, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 12, 13, 14, 16, 17, 18, 19,
           20, 22, 23, 20, 25, 26, 20, 28, 29, 20, 31, 32, 20, 34, 35,
           21, 37, 38, 21, 40, 41, 21, 43, 44, 21, 46, 47, 21, 49, 50]

# SMPL-H joint index -> Unity HumanBodyBones enum name (exact spelling for Enum.Parse).
BONES = ["Hips", "LeftUpperLeg", "RightUpperLeg", "Spine", "LeftLowerLeg", "RightLowerLeg",
         "Chest", "LeftFoot", "RightFoot", "UpperChest", "LeftToes", "RightToes", "Neck",
         "LeftShoulder", "RightShoulder", "Head", "LeftUpperArm", "RightUpperArm",
         "LeftLowerArm", "RightLowerArm", "LeftHand", "RightHand",
         "LeftIndexProximal", "LeftIndexIntermediate", "LeftIndexDistal",
         "LeftMiddleProximal", "LeftMiddleIntermediate", "LeftMiddleDistal",
         "LeftLittleProximal", "LeftLittleIntermediate", "LeftLittleDistal",
         "LeftRingProximal", "LeftRingIntermediate", "LeftRingDistal",
         "LeftThumbProximal", "LeftThumbIntermediate", "LeftThumbDistal",
         "RightIndexProximal", "RightIndexIntermediate", "RightIndexDistal",
         "RightMiddleProximal", "RightMiddleIntermediate", "RightMiddleDistal",
         "RightLittleProximal", "RightLittleIntermediate", "RightLittleDistal",
         "RightRingProximal", "RightRingIntermediate", "RightRingDistal",
         "RightThumbProximal", "RightThumbIntermediate", "RightThumbDistal"]


def _to_flat(a):
    """numpy array (any shape) or (nested) list -> flat python float list."""
    if hasattr(a, "reshape"):                 # numpy
        return a.reshape(-1).tolist()
    flat = []
    for x in a:
        if isinstance(x, (list, tuple)):
            flat.extend(x)
        else:
            flat.append(x)
    return flat


def _aa2quat(rx, ry, rz):                      # == SmplhConverter.AxisAngleToQuat
    theta = math.sqrt(rx * rx + ry * ry + rz * rz)
    if theta < 1e-8:
        return (0.0, 0.0, 0.0, 1.0)
    half = theta * 0.5
    s = math.sin(half) / theta
    return (s * rx, s * ry, s * rz, math.cos(half))


def _qmul(a, b):                               # == Unity Quaternion operator* (a * b)
    ax, ay, az, aw = a
    bx, by, bz, bw = b
    return (aw * bx + ax * bw + ay * bz - az * by,
            aw * by + ay * bw + az * bx - ax * bz,
            aw * bz + az * bw + ax * by - ay * bx,
            aw * bw - ax * bx - ay * by - az * bz)


def _mirror(q):                                # X-mirror: SMPL world -> Unity deformation
    return (q[0], -q[1], -q[2], q[3])


def smplh_to_humanoid(poses, trans, num_frames, framerate=30,
                      prev_trans=None, ref_y=None):
    """Convert SMPL-H motion to humanoid motion.

    poses: n*156 axis-angle (numpy [n,156] or flat/nested list)
    trans: n*3 root translation (numpy [n,3] or flat/nested list)
    Returns the humanoid-format motion dict (HumanoidMotionPlayer schema).

    Streaming continuation (both default to the original whole-clip behavior):
      prev_trans: last [x, y, z] root translation of the PREVIOUS chunk. When
        given, frame 0's root_xz becomes the real step from that frame instead
        of [0, 0], so concatenated chunk conversions equal one whole-clip
        conversion.
      ref_y: pelvis Y of the SESSION's first frame. When given, hips_pos is
        referenced to it instead of this chunk's own frame 0 (prevents a hips
        height jump at every chunk boundary).
    """
    n = int(num_frames)
    out = {
        "num_frames": n,
        "framerate": int(framerate) if framerate else 30,
        "root_xz": [],
        "root_vel_y": [0.0] * n,
        "root_vel_yaw": [0.0] * n,
        "hips_pos": [],
        "joints": {name: [] for name in BONES},
    }
    if n <= 0:
        return out

    pf = _to_flat(poses)
    tf = _to_flat(trans)

    # frame-0 pelvis Y (reference for hips bob), session-pinned when streaming
    t0y = tf[1] if ref_y is None else ref_y
    joints = out["joints"]

    for f in range(n):
        base = f * 156
        world = [None] * NUM_JOINTS
        for j in range(NUM_JOINTS):
            rx = pf[base + j * 3]; ry = pf[base + j * 3 + 1]; rz = pf[base + j * 3 + 2]
            lq = _aa2quat(rx, ry, rz)
            world[j] = lq if j == 0 else _qmul(world[PARENTS[j]], lq)
        for j in range(NUM_JOINTS):
            dq = _mirror(world[j])
            joints[BONES[j]].append([dq[0], dq[1], dq[2], dq[3]])

        sx = tf[f * 3]; sy = tf[f * 3 + 1]; sz = tf[f * 3 + 2]
        if f == 0:
            if prev_trans is None:
                out["root_xz"].append([0.0, 0.0])
            else:  # streaming: real step from the previous chunk's last frame
                out["root_xz"].append([-(sx - prev_trans[0]), (sz - prev_trans[2])])
        else:
            px = tf[(f - 1) * 3]; pz = tf[(f - 1) * 3 + 2]
            out["root_xz"].append([-(sx - px), (sz - pz)])   # X-mirrored frame-to-frame step
        out["hips_pos"].append([0.0, sy - t0y, 0.0])         # vertical bob, referenced to frame 0

    return out
