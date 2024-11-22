import ray
import serve
# Connect to the Ray cluster
ray.init()

# Define a deployment with Ray Serve
@serve.deployment(route_prefix="/hello")
class HelloWorld:
    def __call__(self, request):
        # Define the remote function inside the deployment
        return {"message": ray.get(hello_world.remote())}

# Remote task using Ray
@ray.remote
def hello_world():
    return "Hello, world!"

app = HelloWorld.bind()
serve.run(app)  # This will start the Serve application 


#"import ray; ray.init(); exec(\"\"\"def hello_world():\n    return 'Hello, world!'\nhello_world = ray.remote(hello_world)\nresult = ray.get(hello_world.remote())\nprint(result)\"\"\")"
