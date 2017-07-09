package org.gradoop.metabolism;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;

import com.google.common.base.Preconditions;

public class Metabolism extends AbstractRunner {

	LogicalGraph graph;

	public Metabolism(String[] args) {
		Preconditions.checkArgument(args.length == 1, "input dir required");
		String inputDir = args[0];
		System.out.println(args[0]);

		this.graph = readLogicalGraph(inputDir);
	}

	public String getVertexCount() throws Exception {
		VertexCount vertexCount = new VertexCount();
		graph = graph.aggregate(vertexCount);
		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		return graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey()).toString();
	}

	public String getEdgeCount() throws Exception {
		EdgeCount edgeCount = new EdgeCount();
		graph = graph.aggregate(edgeCount);
		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		return graphHead.getPropertyValue(edgeCount.getAggregatePropertyKey()).toString();
	}

	public void getVertexEdges(int in, int out) throws Exception {

		List<Edge> edges = graph.getEdges().collect();
		List<Vertex> vertices = graph.getVertices().collect();
		// System.out.println("Edges: "+edges.size());
		// System.out.println("Vertices: "+vertices.size());

		Map<Vertex, Integer> EdgesInCounts = new HashMap<>();
		Map<Vertex, Integer> EdgesOutCounts = new HashMap<>();
		int cntIn, cntOut;
		for (Vertex vertex : vertices) {
			GradoopId vertexID = vertex.getId();

			cntIn = 0;
			cntOut = 0;
			for (Edge edge : edges) {
				if (edge.getSourceId().equals(vertexID)) {
					// System.out.println("Source -> "+edge.getSourceId());
					cntOut++;
				}
				if (edge.getTargetId().equals(vertexID)) {
					// //System.out.println("Target -> "+edge.getSourceId());
					cntIn++;
				}
			}

			EdgesInCounts.put(vertex, cntIn);
			EdgesOutCounts.put(vertex, cntOut);
			if (cntIn > in || cntOut > out)
				System.out.println("Vertex: " + vertex.getLabel() + ":\t incoming Edges: " + cntIn
						+ "\t outgoing Edges: " + cntOut);

		}
	}
}

// public static void main(String[] args) throws Exception {
//
//
//
// List<Edge> edges =graph.getEdges().collect(); List<Vertex> vertices =
// graph.getVertices().collect();
// System.out.println("Edges: "+edges.size());
// System.out.println("Vertices: "+vertices.size());
//
// Map<Vertex, Integer> EdgesInCounts = new HashMap<>(); Map<Vertex,
// Integer> EdgesOutCounts = new HashMap<>(); Set<GradoopId> idSet = new
// HashSet<>(); int cntIn, cntOut; for (Vertex vertex : vertices){
// GradoopId vertexID = vertex.getId(); //System.out.println(vertexID);
//
// cntIn = 0; cntOut = 0; for(Edge edge : edges){
// //System.out.println(edge.getSourceId());
// idSet.add(edge.getSourceId()); if
// (edge.getSourceId().equals(vertexID)){
// System.out.println("Source -> "+edge.getSourceId()); cntOut ++; }
// if (edge.getTargetId().equals(vertexID)){
// //System.out.println("Target -> "+edge.getSourceId()); cntIn ++; } }
// EdgesInCounts.put(vertex, cntIn); EdgesOutCounts.put(vertex, cntOut);
// System.out.println("Vertex: "+vertex.getLabel()
// +":\t incoming Edges: "+ cntIn +"\t outgoing Edges: "+ cntOut);
// }
//
// }
