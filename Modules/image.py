from io import BytesIO
import base64

class Dalle3Caller:
    def __init__(self):
        from openai import OpenAI
        from .secrets_chatgpt import API_KEY
        self.client = OpenAI(api_key=API_KEY)
        self.model = "dall-e-3"
        self.size = "1024x1024"

    def call(self, prompt):
        try:
            response = self.client.images.generate(
            model=self.model,
            prompt=prompt,
            size=self.size,
            quality="standard",
            n=1,
            response_format = "b64_json"
            )
            image = response.data[0].b64_json
            image = BytesIO(base64.b64decode(image))
            return image
        except Exception as e:
            print(e)
            return "error"