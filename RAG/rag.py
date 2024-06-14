import requests
import random

# def rag_call(user_input):
#     url = "http://localhost:8080/process_text"
#     data = {"input": user_input}
#     response = requests.post(url, json=data)
#     if response.status_code == 200:
#         result = response.json()
#         prompt = result['context']
#         title = result['title']
#         print("Prompt:", prompt)
#         print("Title:", title)
#     else:
#         print("Error:", response.text)

#     return prompt, title

def rag_call(user_input):
    
    # ramdomly pick a number from 0 to 2
    index = random.randint(0, 2)
    image_name = 'test' + str(index)
    return user_input, image_name
