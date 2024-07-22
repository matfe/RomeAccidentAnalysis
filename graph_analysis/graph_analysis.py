from graphframes import GraphFrame
from pyspark.sql.functions import col, lit


class GraphAnalysis:
    """
    Classe che espone metodi statici per l'analisi di un grafo GraphFrame.

    Per ulteriori dettagli sugli algoritmi di analisi dei grafi, si veda la documentazione di GraphFrames:
    https://graphframes.github.io/graphframes/docs/_site/user-guide.html#graph-algorithms
    """

    @staticmethod
    def analyze(graph: GraphFrame, graph_name, sampleForAdvancedAnalysis = False, sampleFraction = 0.3):
        """
        Analizza il grafo.

        :param graph: Grafo GraphFrame da analizzare
        :param graph_name: Nome del grafo
        :param sampleForAdvancedAnalysis: Se True, campiona il grafo
        :param sampleFraction: Misura di campionamento
        :return: None, stampa i risultati delle analisi
        """

        graph.vertices.cache()
        graph.edges.cache()

        print(f" ----------------- {graph_name}: pagerank  -----------------")
        GraphAnalysis.pagerank(graph).orderBy("pagerank", ascending=False).show()
        print(f" ----------------- {graph_name}: degree  -----------------")
        GraphAnalysis.degree(graph).orderBy("degree", ascending=False).show()
        print(f" ----------------- {graph_name}: in_degree  -----------------")
        GraphAnalysis.in_degree(graph).orderBy("indegree", ascending=False).show()
        print(f" ----------------- {graph_name}: out_degree  -----------------")
        GraphAnalysis.out_degree(graph).orderBy("outdegree", ascending=False).show()

        if sampleForAdvancedAnalysis:
            graph = GraphFrame(graph.vertices.sample(fraction=sampleFraction), graph.edges.sample(fraction=sampleFraction))

        print(f" ----------------- {graph_name}: triangle_count  -----------------")
        GraphAnalysis.triangle_count(graph).show()
        print(f" ----------------- {graph_name}: label_propagation  -----------------")
        GraphAnalysis.label_propagation(graph).show(100)
        print(f" ----------------- {graph_name}: strongly_connected_components  -----------------")
        GraphAnalysis.strongly_connected_components(graph).show(100)
        print(f" ----------------- {graph_name}: motif_finding  -----------------")
        GraphAnalysis.motif_finding(graph).show(100)

    @staticmethod
    def pagerank(graph: GraphFrame):
        """
        Calcola il PageRank del grafo.

        :param graph: Grafo GraphFrame
        :return: DataFrame con i risultati del PageRank
        """
        results = graph.pageRank(resetProbability=0.15, maxIter=10)
        return results.vertices.select("id", "pagerank").orderBy("pagerank", ascending=False)

    @staticmethod
    def degree(graph: GraphFrame):
        """
        Calcola il degree del grafo.

        :param graph: Grafo GraphFrame
        :return: DataFrame con i risultati del degree
        """
        return graph.degrees

    @staticmethod
    def in_degree(graph: GraphFrame):
        """
        Calcola l'in-degree del grafo.

        :param graph: Grafo GraphFrame
        :return: DataFrame con i risultati dell'in-degree
        """
        return graph.inDegrees

    @staticmethod
    def out_degree(graph: GraphFrame):
        """
        Calcola l'out-degree del grafo.

        :param graph: Grafo GraphFrame
        :return: DataFrame con i risultati dell'out-degree
        """
        return graph.outDegrees

    @staticmethod
    def shortest_paths(graph: GraphFrame, landmarks):
        """
        Calcola i percorsi più brevi nel grafo verso i landmarks specificati.

        :param graph: Grafo GraphFrame
        :param landmarks: Lista di landmarks
        :return: DataFrame con i risultati dei percorsi più brevi
        """
        shortest_paths = graph.shortestPaths(landmarks=landmarks)
        return shortest_paths.select("id", "distances")

    @staticmethod
    def triangle_count(graph: GraphFrame):
        """
        Calcola il numero di triangoli nel grafo.

        :param graph: Grafo GraphFrame
        :return: DataFrame con i risultati del conteggio dei triangoli
        """
        results = graph.triangleCount()
        return results.select("id", "count").orderBy("count", ascending=False)

    @staticmethod
    def connected_components(graph: GraphFrame):
        """
        Calcola i componenti connessi del grafo.

        :param graph: Grafo GraphFrame
        :return: DataFrame con i risultati dei componenti connessi
        """
        results = graph.connectedComponents()
        return results.select("id", "component")

    @staticmethod
    def strongly_connected_components(graph: GraphFrame, max_iter=10):
        """
        Calcola i componenti fortemente connessi del grafo.

        :param graph: Grafo GraphFrame
        :param max_iter: Numero massimo di iterazioni
        :return: DataFrame con i risultati dei componenti fortemente connessi
        """
        results = graph.stronglyConnectedComponents(maxIter=max_iter)
        return results.select("id", "component")

    @staticmethod
    def label_propagation(graph: GraphFrame, max_iter=10):
        """
        Esegue l'algoritmo di propagazione delle etichette sul grafo.

        :param graph: Grafo GraphFrame
        :param max_iter: Numero massimo di iterazioni
        :return: DataFrame con i risultati della propagazione delle etichette
        """
        results = graph.labelPropagation(maxIter=max_iter)
        return results.select("id", "label")

    @staticmethod
    def motif_finding(graph: GraphFrame, motif="(a)-[e]->(b)"):
        """
        Trova motivi (patterns) nel grafo.

        :param graph: Grafo GraphFrame
        :param motif: Stringa che specifica il motivo da cercare
        :return: DataFrame con i risultati dei motivi trovati
        """
        results = graph.find(motif)
        return results