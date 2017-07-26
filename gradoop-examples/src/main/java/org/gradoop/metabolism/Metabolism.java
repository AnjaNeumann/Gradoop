
package org.gradoop.metabolism;

import java.time.Instant;
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

	private LogicalGraph graph;

	private String inputDir;

	private Map<Vertex, Integer> edgesInCounts = new HashMap<>();

	private Map<Vertex, Integer> edgesOutCounts = new HashMap<>();

	private Map<GradoopId, Vertex> vertexMap;

	private Map<Vertex, List<Edge>> edgesOutMap;

	private LinkedList<GraphSet> paths;

	private ExecutionEnvironment env;

	private Map<Vertex, GraphSet> longestPathMap;

	private List<Vertex> visitedVertices;

	/**
	 * Constructor
	 * 
	 * @param args:
	 *            input path
	 */
	public Metabolism(String[] args) {

		Preconditions.checkArgument(args.length == 1, "input dir required");
		inputDir = args[0];
		System.out.println(args[0]);
		env = ExecutionEnvironment.getExecutionEnvironment();
		this.graph = readLogicalGraph(inputDir);

	}

	/**
	 * graph getter
	 * 
	 * @return logical graph
	 */
	public LogicalGraph getGraph() {
		return graph;
	}

	/**
	 * find all graphs with label "subsystem" and write them as a new graph to
	 * file
	 * 
	 * @throws Exception
	 */
	public void getSubsystems() throws Exception {

		String graphHeadFile = inputDir + "/graphs.json";
		String vertexFile = inputDir + "/vertices.json";
		String edgeFile = inputDir + "/edges.json";

		// ExecutionEnvironment env =
		// ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

		// writeLogicalGraph(graph, inputDir + "/graphs");
		JSONDataSource dataSource = new JSONDataSource(graphHeadFile, vertexFile, edgeFile, config);

		// read graph collection from DataSource
		GraphCollection graphCollection = dataSource.getGraphCollection();

		@SuppressWarnings("serial")
		GraphCollection filtered = graphCollection.select(new FilterFunction<GraphHead>() {
			@Override
			public boolean filter(GraphHead g) {
				return g.getLabel().equals("subsystem");
			}
		});

		filtered.writeTo(getJDataSink("allSubsystems"));

		env.execute();

	}

	/**
	 * find longest path (max of all subpaths) in a logical graph and write it
	 * as logical graph to file
	 * 
	 * @throws Exception
	 */
	public void getLogestPath() throws Exception {

		List<Vertex> vertices = graph.getVertices().collect();
		longestPathMap = new HashMap<>(vertices.size());
		vertexMap = new HashMap<>(vertices.size());
		edgesOutMap = new HashMap<>(vertices.size());
		if (edgesInCounts.isEmpty())
			getVertexEdges(0, 0);

		Set<Vertex> sourceSet = new HashSet<>();
		for (Vertex v : vertices) {

			vertexMap.put(v.getId(), v);
			edgesOutMap.put(v, graph.getOutgoingEdges(v.getId()).collect());
			if (edgesInCounts.get(v).equals(0) && v.getPropertyValue("type").toString().equals("metabolite")) {

				// System.out.println(v.getLabel());
				sourceSet.add(v);
			}
		}

		paths = new LinkedList<>();
		Instant start = Instant.now();
		for (Vertex v : sourceSet) {
			System.out.println(v.getLabel() + ": calculating path");
			visitedVertices = new LinkedList<>();
			GraphSet graphSet = new GraphSet();
			graphSet = getTargetSet(v);
			paths.add(graphSet);

			System.out.println(graphSet.getVertexCount());

		}

		// GraphSet longestPath = sourceSet.parallelStream().map(node ->
		// getTargetSet(node))
		// .peek(gs -> System.out.println("Graphset with " + gs.getVertexCount()
		// + " vertices processed."))
		// .max((gs1, gs2) -> Integer.compare(gs1.getVertexCount(),
		// gs2.getVertexCount())).orElse(null);

		Long millisecondsTaken = Instant.now().toEpochMilli() - start.toEpochMilli();

		System.out.println("Finding all partial graphs took " + millisecondsTaken + "ms.");

		int cnt = 0;
		GraphSet gs = null;
		for (GraphSet set : paths) {
			if (set.getVertexCount() > cnt) {
				cnt = set.getVertexCount();
				gs = set;
			}
		}

		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
		LogicalGraph lg = LogicalGraph.fromCollections(new GraphHead(GradoopId.get(), "longestPath", null),
				gs.getVertices(), gs.getEdges(), config);

		lg.writeTo(getJDataSink("longestPath"));
		env.execute();

	}

	/**
	 * creates a new JSONDataSink (to write a logical graph to file)
	 * 
	 * @param folder:
	 *            target folder
	 * @return
	 */
	private JSONDataSink getJDataSink(String folder) {

		String graphHeadFile = inputDir + "/" + folder + "/graphs.json";
		String vertexFile = inputDir + "/" + folder + "/vertices.json";
		String edgeFile = inputDir + "/" + folder + "/edges.json";

		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

		return new JSONDataSink(graphHeadFile, vertexFile, edgeFile, config);

	}

	/**
	 * find connected subgraph with source vertex
	 * 
	 * @param vertex:
	 *            source
	 * @param graphSet:
	 *            collection of subgraph vertices and edges
	 * @return subgraph
	 */
	private GraphSet getSubgraphSet(Vertex vertex, GraphSet graphSet) {

		List<Edge> out = edgesOutMap.get(vertex);
		for (Edge edge : out) {

			graphSet.addEdge(edge);
			Vertex target = vertexMap.get(edge.getTargetId());
			if (!graphSet.containsVertex(target)) {

				graphSet.addVertex(target);
				graphSet = getSubgraphSet(target, graphSet);
			}
		}

		return graphSet;

	}

	/**
	 * find longest Path collection with source vertex
	 * 
	 * @param vertex
	 * @return longest Path collection
	 */
	private GraphSet getTargetSet(Vertex vertex) {
		List<Edge> out = edgesOutMap.get(vertex);
		visitedVertices.add(vertex);
		int max = -1;
		Edge input = null;
		GraphSet output = new GraphSet();

		for (Edge edge : out) {
			GraphSet graphSetPart = new GraphSet();
			Vertex target = vertexMap.get(edge.getTargetId());
			if (!visitedVertices.contains(target)) {
				if (!longestPathMap.containsKey(target)) {
					graphSetPart = getTargetSet(target);
					longestPathMap.put(target, graphSetPart);
				} else
					graphSetPart = longestPathMap.get(target);
			}
			if (graphSetPart.getEdgeCount() > max) {
				output = graphSetPart.copy();
				input = edge;
				max = graphSetPart.getVertexCount();
			}
		}
		if (input != null)
			output.addEdge(input);

		output.addVertex(vertex);
		return output;

	}

	/**
	 * writes all logical graphs of type subsystem to separate file
	 * 
	 * @throws Exception
	 */
	public void writeSubsystems2File() throws Exception {

		String graphHeadFile = inputDir + "/graphs.json";
		String vertexFile = inputDir + "/vertices.json";
		String edgeFile = inputDir + "/edges.json";
		// ExecutionEnvironment env =
		// ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
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

				// execute program
				env.execute();

			}
		}
	}

	/**
	 * add Property VertexCount to graph
	 * 
	 * @return VertexCount
	 * @throws Exception
	 */
	public String getVertexCount() throws Exception {
		VertexCount vertexCount = new VertexCount();
		graph = graph.aggregate(vertexCount);
		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		return graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey()).toString();

	}

	/**
	 * add Property EdgeCount to graph
	 * 
	 * @return EdgeCount
	 * @throws Exception
	 */
	public String getEdgeCount() throws Exception {
		EdgeCount edgeCount = new EdgeCount();
		graph = graph.aggregate(edgeCount);
		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		return graphHead.getPropertyValue(edgeCount.getAggregatePropertyKey()).toString();
	}

	public void getMembraneReactionGraph() throws Exception {
		System.out.println(graph.match("(n1)-[:gene_or]->(n2)").getVertices().count());

	}

	/**
	 * add VertexCount and EdgeCount to graph properties and print counts of
	 * incoming and outgoing Edges for each vertex if they are bigger than their
	 * thresholds in and out fill global hashmaps edgesInCounts and
	 * edgesOutCounts
	 * 
	 * @param in:
	 *            threshold for incoming edges
	 * @param out:
	 *            threshold for outgoing edges
	 * @throws Exception
	 */
	public void getVertexEdges(int in, int out) throws Exception {
		getEdgeCount();
		getVertexCount();
		List<Edge> edges = graph.getEdges().collect();
		List<Vertex> vertices = graph.getVertices().collect();
		// System.out.println("Edges: "+edges.size());
		// System.out.println("Vertices: "+vertices.size());
		edgesInCounts = new HashMap<>();
		edgesOutCounts = new HashMap<>();
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
			edgesInCounts.put(vertex, cntIn);
			edgesOutCounts.put(vertex, cntOut);
			if (cntIn > in || cntOut > out)
				System.out.println("Vertex: " + vertex.getLabel() + ":\t incoming Edges: " + cntIn
						+ "\t outgoing Edges: " + cntOut);
		}
	}

	/**
	 * groups graph by clusterId (subsystems)
	 * 
	 * @throws Exception
	 */
	public void grouping() throws Exception {

		List<String> keys = new LinkedList<>();
		keys.add("ClusterId");
		LogicalGraph grouped = graph.groupBy(keys);
		System.out.println(grouped.getVertices().collect().size());
		System.out.println(grouped.getEdges().collect().size());
		writeLogicalGraph(grouped, "src/main/resources/data/json/Metabolism/GroupedGraph");

	}

}
