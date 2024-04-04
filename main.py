import subprocess

# Define the list of required packages
packages = ["fastapi", "uvicorn"]

# Install the packages using pip
for package in packages:
    subprocess.run(["pip3", "install", package], check=True)

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"Welcome": "to the MeatyAPIs FastAPI service!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
