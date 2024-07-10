import heapq
import os
import traceback
import psycopg2
import random
from collections import defaultdict
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Function to establish a connection to the database
def connect_to_db():
    connection = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    return connection

def load_graph_from_db(cursor, graph):
    cursor.execute("SELECT id_node FROM nodes WHERE id_topology = 1")
    nodes = cursor.fetchall()
    for node in nodes:
        graph.add_node(node['id_node'])

    cursor.execute("SELECT source, target FROM links WHERE id_topology = 1")
    links = cursor.fetchall()
    for link in links:
        distance = random.randint(1, 10)  # Replace with actual distance if available
        graph.add_edge(link['source'], link['target'], distance, [])

# Define the graph structure
class Graph:
    def __init__(self):
        self.nodes = set()
        self.edges = defaultdict(list)
        self.distances = {}
        self.srlgs = defaultdict(set)

    def add_node(self, value):
        self.nodes.add(value)

    def add_edge(self, from_node, to_node, distance, srlg_ids):
        self.edges[from_node].append(to_node)
        self.edges[to_node].append(from_node)  # If it's bidirectional
        self.distances[(from_node, to_node)] = distance
        self.distances[(to_node, from_node)] = distance  # If it's bidirectional

# A simple implementation of Dijkstra's Algorithm
def dijkstra(graph, start, excluded_edges=set()):
    distances = {node: float('inf') for node in graph.nodes}
    distances[start] = 0
    queue = [(0, start)]
    paths = {node: [] for node in graph.nodes}
    paths[start] = [start]

    while queue:
        current_distance, current_node = heapq.heappop(queue)
        for neighbor in graph.edges[current_node]:
            if (current_node, neighbor) in excluded_edges or (neighbor, current_node) in excluded_edges:
                continue
            distance = graph.distances[(current_node, neighbor)]
            new_distance = current_distance + distance
            if new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                heapq.heappush(queue, (new_distance, neighbor))
                paths[neighbor] = paths[current_node] + [neighbor]

    return distances, paths

# A function to find k shortest disjoint paths using a modified version of Dijkstra's Algorithm
def find_k_shortest_paths(graph, start, end, k):
    all_paths = []
    excluded_edges = set()

    for _ in range(k):
        distances, paths = dijkstra(graph, start, excluded_edges)
        if end not in paths or not paths[end]:
            break
        path = paths[end]
        all_paths.append(path)
        for edge in zip(path[:-1], path[1:]):
            excluded_edges.add(edge)
            excluded_edges.add((edge[1], edge[0]))

    return all_paths

def insert_path_into_db(cursor, id_topology, path, path_type):
    points_wkt = []
    for node_id in path:
        cursor.execute("SELECT ST_AsText(point) AS point FROM nodes WHERE id_node = %s;", (node_id,))
        node_result = cursor.fetchone()
        if node_result:
            point_coords = node_result['point'].strip('POINT()')
            points_wkt.append(point_coords)
        else:
            raise Exception(f"No se encontró el nodo con ID: {node_id}")

    linestring_wkt = f"LINESTRING({', '.join(points_wkt)})"

    try:
        cursor.execute("""
            INSERT INTO routes_kspf (id_topology, source, target, path_type, geom)
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326));
        """, (id_topology, path[0], path[-1], path_type, linestring_wkt))
    except Exception as e:
        print("Error insertando ruta en la base de datos:", e)
        raise

def main():
    conn = connect_to_db()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            g = Graph()
            load_graph_from_db(cursor, g)
            src, dst = 1, 20  # Example source and destination nodes
            k = 3  # Number of paths to find
            paths = find_k_shortest_paths(g, src, dst, k)

            print("Rutas encontradas:", paths)

            for i, path in enumerate(paths):
                path_type = "primary" if i == 0 else "backup"
                insert_path_into_db(cursor, 1, path, path_type)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Se produjo un error: {e}")
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
