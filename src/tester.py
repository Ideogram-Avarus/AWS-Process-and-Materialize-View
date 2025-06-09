from dotenv import load_dotenv
from lambda_function import lambda_handler

load_dotenv()


if __name__ == "__main__":
    lambda_handler({}, None)

