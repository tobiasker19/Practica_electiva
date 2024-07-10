import psycopg2
import networkx as nx
from scipy.spatial import KDTree
from dotenv import load_dotenv
import os
import numpy as np
import itertools

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Conectar a la base de datos PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

def read_topology(conn):
    G = nx.Graph()

    with conn.cursor() as cursor:
        # Leer nodos
        cursor.execute("SELECT id_node, lat, lon FROM nodes WHERE id_topology = 1")
        nodes = cursor.fetchall()
        for node in nodes:
            node_id, lat, lon = node
            G.add_node(node_id, pos=(lon, lat))
        
        # Leer enlaces
        cursor.execute("SELECT id_link, source_node, target_node, ST_Length(geom::geography) AS length FROM links WHERE id_topology = 1")
        links = cursor.fetchall()
        for link in links:
            id_link, source_node, target_node, length = link
            print(f"Adding edge: {source_node} -> {target_node} with length {length}")
            G.add_edge(source_node, target_node, weight=length, id=id_link)

    return G

G = read_topology(conn)

def yen_k_shortest_paths(graph, source, target, K):
    print("Finding K shortest paths...")
    try:
        paths = list(itertools.islice(nx.shortest_simple_paths(graph, source, target, weight='weight'), K))
    except Exception as e:
        print(f"Error finding paths: {e}")
        paths = []
    print(f"Found {len(paths)} paths.")
    return paths

def paths_are_disjoint(path1, path2, min_distance, graph):
    # Verificar si los caminos son disjuntos en términos de nodos y enlaces
    nodes_disjoint = set(path1).isdisjoint(set(path2))
    edges1 = {(min(path1[i], path1[i + 1]), max(path1[i], path1[i + 1])) for i in range(len(path1) - 1)}
    edges2 = {(min(path2[i], path2[i + 1]), max(path2[i], path2[i + 1])) for i in range(len(path2) - 1)}
    edges_disjoint = edges1.isdisjoint(edges2)

    # Verificar si la distancia mínima entre los caminos es mayor o igual al min_distance
    msd = compute_msd(path1, path2, graph)
    distance_disjoint = msd >= min_distance

    print(f"Paths {path1} and {path2} are disjoint in nodes: {nodes_disjoint}, disjoint in edges: {edges_disjoint}, distance disjoint: {distance_disjoint}")
    return nodes_disjoint and edges_disjoint and distance_disjoint

def compute_msd(path1, path2, graph):
    points1 = np.array([graph.nodes[node]['pos'] for node in path1])
    points2 = np.array([graph.nodes[node]['pos'] for node in path2])
    
    tree1 = KDTree(points1)
    tree2 = KDTree(points2)
    
    msd = float('inf')
    for point in points1:
        distance, _ = tree2.query(point)
        msd = min(msd, distance)
    
    for point in points2:
        distance, _ = tree1.query(point)
        msd = min(msd, distance)
    
    return msd

def find_best_disjoint_pair(graph, k_paths, min_distance):
    print("Finding best disjoint path pair...")
    max_msd = 0
    best_pair = None
    
    for i in range(len(k_paths)):
        for j in range(i + 1, len(k_paths)):
            path1 = k_paths[i]
            path2 = k_paths[j]
            # Verificar si los caminos son disjuntos y cumplen con la distancia mínima
            if paths_are_disjoint(path1, path2, min_distance, graph):
                print(f"Checking disjoint paths: {path1} and {path2}")
                msd = compute_msd(path1, path2, graph)
                print(f"MSD between paths: {msd}")
                if msd > max_msd:
                    max_msd = msd
                    best_pair = (path1, path2)
    
    print("Finished finding best disjoint path pair.")
    return best_pair

def save_route_to_db(conn, id_topology, route_type, path):
    with conn.cursor() as cursor:
        # Construir el LINESTRING para la ruta
        linestring = "LINESTRING("
        for node in path:
            lon, lat = G.nodes[node]['pos']
            linestring += f"{lon} {lat}, "
        linestring = linestring.rstrip(", ") + ")"
        
        cursor.execute("""
            INSERT INTO routes_dpmmsd (id_topology, route_type, geom)
            VALUES (%s, %s, ST_GeomFromText(%s, 4326))
        """, (id_topology, route_type, linestring))
    
    conn.commit()

source = 1
destination = 20
K = 3  # Incrementar el número de caminos más cortos a considerar
min_distance = 1000000000000000  # Ajustar la distancia mínima deseada (en metros)

print("Starting algorithm DPMMSD...")
k_paths = yen_k_shortest_paths(G, source, destination, K)
print(f"Paths found: {k_paths}")
best_pair = find_best_disjoint_pair(G, k_paths, min_distance)

if best_pair:
    primary_path, backup_path = best_pair
    print("Ruta primaria:", primary_path)
    print("Ruta de respaldo:", backup_path)

    save_route_to_db(conn, 1, 'primary', primary_path)
    save_route_to_db(conn, 1, 'backup', backup_path)
else:
    print("No se encontraron rutas disjuntas.")
