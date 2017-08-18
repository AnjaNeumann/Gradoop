package org.gradoop.metabolism;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * class to collect vertices and edges (to build logical graph from collections)
 *
 */
public class GraphSet {
	private List<Vertex> vertices;
	private List<Edge> edges;

	/**
	 * constructs class with empty lists
	 */
	public GraphSet() {
		this(new LinkedList<Vertex>(), new LinkedList<Edge>());
	}

	/**
	 * constructs class with start vertex, {@link vertices} get one element
	 */
	public GraphSet(Vertex first) {
		this();
		this.addVertex(first);
	}

	/**
	 * constructs class and set {@link vertices} and and {@link edges}
	 * 
	 * @param vertices
	 * @param edges
	 */
	public GraphSet(List<Vertex> vertices, List<Edge> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	/**
	 * return list of current vertices
	 * 
	 * @return {@link vertices}
	 */
	public List<Vertex> getVertices() {
		return vertices;
	}

	/**
	 * return list of current edges
	 * 
	 * @return {@link edges}
	 */
	public List<Edge> getEdges() {
		return edges;
	}

	/**
	 * add {@code vertex} to {@link vertices}
	 * 
	 * @param vertex
	 *            new vertex to be append to {@link vertices}
	 */
	public void addVertex(Vertex vertex) {
		vertices.add(vertex);
	}

	/**
	 * add {@code edge} to {@link edges}
	 * 
	 * @param edge
	 *            new edge to be append to {@link edges}
	 */
	public void addEdge(Edge edge) {
		edges.add(edge);
	}

	/**
	 * check if {@link vertices} contains {@code vertex}
	 * 
	 * @param vertex
	 * @return true if condition is satisfied or false otherwise
	 */
	public boolean containsVertex(Vertex vertex) {
		return vertices.contains(vertex);
	}

	/**
	 * 
	 * @return size of {@link vertices}
	 */
	public int getVertexCount() {
		return vertices.size();
	}

	/**
	 * 
	 * @return size of {@link edges}
	 */
	public int getEdgeCount() {
		return edges.size();
	}

	/**
	 * makes a copy of {@link edges}
	 * 
	 * @return copied edge list
	 */
	private List<Edge> copyEdges() {
		List<Edge> copyEdges = new ArrayList<Edge>(edges.size() + 1);
		copyEdges.addAll(edges);
		return copyEdges;
	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public LogicalGraph getLogicalGraph() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
		return LogicalGraph.fromDataSets(null, (DataSet<Vertex>) vertices, (DataSet<Edge>) edges, config);

	}

	/**
	 * makes a copy of itself
	 * 
	 * @return copy of this instance of {@code GraphSet}
	 */
	public GraphSet copy() {
		return new GraphSet(copyVertices(), copyEdges());
	}

	/**
	 * makes a copy of {@link vertices}
	 * 
	 * @return copied vertex list
	 */
	private List<Vertex> copyVertices() {
		List<Vertex> verticesCopy = new ArrayList<>(vertices.size() + 1);
		verticesCopy.addAll(vertices);
		return verticesCopy;
	}
}
