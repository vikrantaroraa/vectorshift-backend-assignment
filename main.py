from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from collections import deque, defaultdict

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Function to check if the graph is a DAG
def is_dag(nodes, edges):
    # Create a graph using adjacency lists and initialize in-degrees
    graph = defaultdict(list)
    in_degree = {node: 0 for node in nodes}

    # Populate the graph and in-degrees
    for u, v in edges:
        graph[u].append(v)
        in_degree[v] += 1

    # Use a queue to perform Kahn's algorithm
    queue = deque([node for node in nodes if in_degree[node] == 0])
    count_visited = 0

    while queue:
        current = queue.popleft()
        count_visited += 1

        # For each neighbor, reduce its in-degree and if it becomes zero, add it to the queue
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # If we visited all the nodes, the graph is a DAG
    return count_visited == len(nodes)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
def parse_pipeline(pipeline: dict) -> dict:
    nodes = pipeline.get("nodes")
    edges = pipeline.get("edges")
    # print("pipeline hai ye: ", pipeline)
    # print("nodes hai ye", nodes)
    # print("edges hai ye", edges)

    # Extract nodes and edges from the array
    nodes_set = set()
    edges_list = []

    for edge in edges:
        source = edge['source']
        target = edge['target']
        nodes_set.add(source)
        nodes_set.add(target)
        edges_list.append((source, target))

    print(f"Nodes: {nodes_set}")
    print(f"Edges: {edges_list}")
    # Check if the provided graph is a DAG
    is_dag_result = is_dag(nodes_set, edges_list)
    print(f"Is the graph a DAG? {is_dag_result}")
    return {"num_nodes": len(nodes), "num_edges": len(edges), "is_dag": is_dag_result}
