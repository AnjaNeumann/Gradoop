package org.gradoop.metabolism;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

import com.google.common.base.Preconditions;

public class Metabolism extends AbstractRunner {

	LogicalGraph graph;
	String inputDir;
	Map<Vertex, Integer> EdgesInCounts;
	Map<Vertex, Integer> EdgesOutCounts;
	Map<GradoopId, Vertex> vertexMap;
	LinkedList<GraphSet> paths;

	public LogicalGraph getGraph() {
		return graph;
	}

	@SuppressWarnings("serial")
	public void getSubsystems() throws Exception {

		FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) {
				return v.getGraphIds().contains(GradoopId.fromString("5964c0f0c7edff2ede4d4ca1"));
			}
		};
		FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {

			@Override
			public boolean filter(Edge e) {
				return e.getGraphIds().contains(GradoopId.fromString("5964c0f0c7edff2ede4d4ca1"));
			}
		};

		LogicalGraph subgraph = graph.subgraph(vertexFilterFunction, edgeFilterFunction);

		List<Vertex> vertices = subgraph.getVertices().collect();
		for (Vertex v : vertices) {

			System.out.println("type: " + v.getPropertyValue("type") + ", name: " + v.getLabel());

		}

		// writeLogicalGraph(subgraph, inputDir + "/subgraph");

	}

	public void getLogestPath() throws Exception {
		vertexMap = new HashMap<>();

		List<Vertex> vertices = graph.getVertices().collect();
		Set<Vertex> sourceSet = new HashSet<>();
		for (Vertex v : vertices) {
			vertexMap.put(v.getId(), v);
			if (EdgesInCounts.get(v).equals(0) && v.getPropertyValue("type").toString().equals("metabolite")) {
				System.out.println(v.getLabel());
				sourceSet.add(v);
			}
		}

		paths = new LinkedList<>();

		for (Vertex v : sourceSet) {
			GraphSet graphSet = new GraphSet();

			graphSet.addVertex(v);
			graphSet = getTargetSet(v, graphSet);
			paths.add(graphSet);
			System.out.println(graphSet.getVertexCount());

		}
		int cnt = 0;
		GraphSet gs = null;
		for (GraphSet set : paths) {
			int len = set.longestPathSize();
			if (len > cnt) {
				cnt = len;
				gs = set;
			}
			System.out.println("vertices: " + set.getVertexCount() + " \tedges: " + set.getEdgeCount()
					+ " \tlongestPath: " + set.longestPathSize());
		}
		// LogicalGraph lonPath = gs.getLogicalGraph();
		// writeLogicalGraph(lonPath, inputDir + "/longestPath");
		// lonPath.writeTo();

	}

	private JSONDataSink getJDataSink(String folder) {
		String graphHeadFile = inputDir + "/" + folder + "/graphs.json";
		String vertexFile = inputDir + "/" + folder + "/vertices.json";
		String edgeFile = inputDir + "/" + folder + "/edges.json";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

		// writeLogicalGraph(graph, inputDir + "/graphs");

		return new JSONDataSink(graphHeadFile, vertexFile, edgeFile, config);
	}

	private GraphSet getTargetSet(Vertex vertex, GraphSet graphSet) throws Exception {
		// Set<Vertex> vertexSet = new HashSet<>();
		List<Edge> out = graph.getOutgoingEdges(vertex.getId()).collect();
		for (Edge edge : out) {
			graphSet.addEdge(edge);
			Vertex target = vertexMap.get(edge.getTargetId());
			if (!graphSet.containsVertex(target)) {
				graphSet.addVertex(target);
				graphSet = getTargetSet(target, graphSet);
			}

		}
		return graphSet;

	}

	public void writeSubsystems2File() throws Exception {

		String graphHeadFile = inputDir + "/graphs.json";
		String vertexFile = inputDir + "/vertices.json";
		String edgeFile = inputDir + "/edges.json";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

		// writeLogicalGraph(graph, inputDir + "/graphs");

		JSONDataSource dataSource = new JSONDataSource(graphHeadFile, vertexFile, edgeFile, config);

		// read graph collection from DataSource
		GraphCollection graphCollection = dataSource.getGraphCollection();
		List<GraphHead> graphHeads = graphCollection.getGraphHeads().collect();

		for (GraphHead gh : graphHeads) {
			if (gh.getLabel().equals("subsystem")) {

				LogicalGraph subgraph = graphCollection.getGraph(gh.getId());

				String subsystemName = gh.getPropertyValue("name").toString().trim().replaceAll(" |,|:|;|\\/", "_");

				String graphs = inputDir + "/subsystems/" + subsystemName + "/graphHeads.json";
				String vertices = inputDir + "/subsystems/" + subsystemName + "/vertices.json";
				String edges = inputDir + "/subsystems/" + subsystemName + "/edges.json";

				subgraph.writeTo(new JSONDataSink(graphs, vertices, edges, config));
				//
				// execute program
				env.execute();
			}

		}

	}

	public Metabolism(String[] args) {
		Preconditions.checkArgument(args.length == 1, "input dir required");
		inputDir = args[0];
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
	
	public void getMembraneReactionGraph() throws Exception {
		System.out.println(graph.match("(n1)-[:gene_or]->(n2)").getVertices().count());
		
	}

	public void getVertexEdges(int in, int out) throws Exception {

		List<Edge> edges = graph.getEdges().collect();
		List<Vertex> vertices = graph.getVertices().collect();
		// System.out.println("Edges: "+edges.size());
		// System.out.println("Vertices: "+vertices.size());

		EdgesInCounts = new HashMap<>();
		EdgesOutCounts = new HashMap<>();
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
