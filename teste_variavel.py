from dotenv import load_dotenv
import os

# pip install python-dotenv
# create file .env root path project
# load variables
load_dotenv()
print(os.environ.get('PYARROW_IGNORE_TIMEZONE'))



