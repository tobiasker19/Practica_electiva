import heapq
import os
import traceback
import random
import psycopg2
from collections import defaultdict
from psycopg2.extras import RealDictCursor, execute_values
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
        graph.add_node(node['id_node'])  # Aquí usamos la clave 'id_node' porque el resultado es un diccionario

    cursor.execute("SELECT source, target FROM links WHERE id_topology = 1")
    links = cursor.fetchall()
    for link in links:
        distance = random.randint(1, 10)  # Asignamos un costo aleatorio
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
        for srlg_id in srlg_ids:
            self.srlgs[(from_node, to_node)].add(srlg_id)
            self.srlgs[(to_node, from_node)].add(srlg_id)  # If it's bidirectional

# A simple implementation of Dijkstra's Algorithm
def dijkstra(graph, start):
    # Initialize distances as infinity and the start node distance as zero
    distances = {node: float('inf') for node in graph.nodes}
    distances[start] = 0
    
    # Priority queue to keep track of the minimum distance nodes
    queue = [(0, start)]
    
    # Keep track of the paths
    paths = {node: [] for node in graph.nodes}
    paths[start] = [start]
    
    while queue:
        current_distance, current_node = heapq.heappop(queue)
        
        for neighbor in graph.edges[current_node]:
            distance = graph.distances[(current_node, neighbor)]
            new_distance = current_distance + distance
            if new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                heapq.heappush(queue, (new_distance, neighbor))
                paths[neighbor] = paths[current_node] + [neighbor]
    
    return distances, paths

# A function to find k shortest disjoint paths using a modified version of Dijkstra's Algorithm
def find_k_shortest_paths(graph, start, end, k):
    primary_path = None
    backup_paths = []

    # First, find the primary path using Dijkstra's algorithm
    distances, all_paths = dijkstra(graph, start)
    if end in all_paths and all_paths[end]:
        primary_path = all_paths[end]
        # Remove primary path edges from the graph
        for edge in zip(primary_path[:-1], primary_path[1:]):
            graph.distances[edge] += 100000  # Increase the weight to make it practically unusable
    else:
        return None, []  # No primary path found, return None for primary path and empty list for backups

    # Now, find backup paths
    for _ in range(k - 1):
        distances, all_paths = dijkstra(graph, start)
        if end in all_paths and all_paths[end] not in backup_paths + [primary_path]:
            backup_path = all_paths[end]
            backup_paths.append(backup_path)
            # Increase the weights of used edges to find disjoint paths
            for edge in zip(backup_path[:-1], backup_path[1:]):
                graph.distances[edge] += 100000

    return primary_path, backup_paths

def insert_path_into_db(cursor, id_topology, path, path_type):
    # Convertir los identificadores de los nodos en una lista de puntos WKT (Well-Known Text)
    points_wkt = []
    for node_id in path:
        # Obtiene la representación de texto del punto de cada nodo por su ID
        cursor.execute("SELECT ST_AsText(point) AS point FROM nodes WHERE id_node = %s;", (node_id,))
        node_result = cursor.fetchone()
        if node_result:
            # Extraer solo las coordenadas del punto
            point_coords = node_result['point'].strip('POINT()')
            points_wkt.append(point_coords)
        else:
            # Si no se encuentra el nodo, se lanza una excepción
            raise Exception(f"No se encontró el nodo con ID: {node_id}")

    # Crear un WKT de la línea que une los puntos
    linestring_wkt = f"LINESTRING({', '.join(points_wkt)})"

    try:
        # Insertar la ruta en la base de datos
        cursor.execute("""
            INSERT INTO routes (id_topology, source, target, path_type, geom)
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326));
        """, (id_topology, path[0], path[-1], path_type, linestring_wkt))
    except Exception as e:
        print("Error insertando ruta en la base de datos:", e)
        raise


# Assuming a Graph class is already defined as in the previous code
# Now let's use the above functions in the main execution

def main():
    # Establecer una conexión a la base de datos
    conn = connect_to_db()
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Crear una instancia del grafo
            g = Graph()
            
            # Poblar el grafo con nodos y aristas de la base de datos
            load_graph_from_db(cursor, g)
            
            # Encontrar k caminos más cortos
            src, dst = 1, 10  # Nodo de inicio y destino de ejemplo
            k = 3  # Número de rutas a encontrar
            primary_path, backup_paths = find_k_shortest_paths(g, src, dst, k)
            
            # Procesar las rutas
            print("Ruta Principal:", primary_path)
            print("Rutas de Respaldo:", backup_paths)
            
            # Insertar la ruta principal en la base de datos
            if primary_path:
                insert_path_into_db(cursor, 1, primary_path, "primary")
            
            # Insertar rutas de respaldo en la base de datos
            for backup_path in backup_paths:
                insert_path_into_db(cursor, 1, backup_path, "backup")
        
        # Confirmar las inserciones si todo salió bien
        conn.commit()
    except Exception as e:
        # Revertir las inserciones si hubo un error
        conn.rollback()
        print(f"Se produjo un error: {e}")
        traceback.print_exc()  # Imprime la traza del error
    finally:
        # Cerrar la conexión a la base de datos
        conn.close()

if __name__ == "__main__":
    main()