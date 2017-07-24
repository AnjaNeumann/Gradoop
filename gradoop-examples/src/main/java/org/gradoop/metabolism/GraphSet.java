package org.gradoop.metabolism;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class GraphSet {
	private List<Vertex> vertices;
	private List<Edge> edges;
	private Map<GradoopId, Vertex> vertexMap;

	public GraphSet() {
		this(new LinkedList<Vertex>(), new LinkedList<Edge>());
	}

	public GraphSet(List<Vertex> vertices, List<Edge> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	public List<Vertex> getVertices() {
		return vertices;
	}

	public List<Edge> getEdges() {
		return edges;
	}

	public void addVertex(Vertex vertex) {
		vertices.add(vertex);
	}

	public void addEdge(Edge edge) {
		edges.add(edge);
	}

	public boolean containsVertex(Vertex vertex) {
		return vertices.contains(vertex);
	}

	public int getVertexCount() {
		return vertices.size();
	}

	public int getEdgeCount() {
		return edges.size();
	}

	private List<Edge> copyEdges() {
		List<Edge> copyEdges = new LinkedList<Edge>();
		copyEdges.addAll(edges);
		return copyEdges;
	}

	private void setVertexMap() {
		vertexMap = new HashMap<>();
		for (Vertex v : vertices) {
			vertexMap.put(v.getId(), v);
		}
	}

	private int getPathLength(Vertex vertex, List<Edge> copyEdges) {

		List<Edge> edgesOut = new LinkedList<>();
		for (Edge edge : copyEdges) {
			if (edge.getSourceId().equals(vertex.getId())) {
				edgesOut.add(edge);
			}
		}
		copyEdges.removeAll(edgesOut);
		List<Integer> depths = new ArrayList<>(edgesOut.size());
		for (Edge edge : edgesOut) {
			Vertex target = vertexMap.get(edge.getTargetId());
			depths.add(getPathLength(target, copyEdges) + 1);
		}
		if (depths.isEmpty()) {
			return 0;
		}
		return Collections.max(depths);
	}

	public int longestPathSize() {
		setVertexMap();
		List<Edge> copyEdges = copyEdges();

		Vertex vertex = vertices.get(0);
		return getPathLength(vertex, copyEdges);

	}

	@SuppressWarnings("unchecked")
	public LogicalGraph getLogicalGraph() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
		return LogicalGraph.fromDataSets(null, (DataSet<Vertex>) vertices, (DataSet<Edge>) edges, config);

	}
}
