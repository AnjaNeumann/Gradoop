
package org.gradoop.metabolism;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

import com.google.common.base.Preconditions;

/**
 * perform some gradoop operations for BiGG model data, input: path to EPGM
 *
 */
public class Metabolism extends AbstractRunner {
	/** Property key for cluster ids */
	private static final String CLUSTER_ID_KEY = "ClusterId";
	/** Label to filter the graph headers for subsystems */
	private static final String SUBSYSTEM_LABEL = "subsystem";
	/** Folder name where the graph collection will be stored */
	public static final String ALL_SUBSYSTEMS = "allSubsystems";
	private LogicalGraph graph;
	private String inputDir;
	private Map<Vertex, Integer> edgesInCounts = new HashMap<>();
	private Map<Vertex, Integer> edgesOutCounts = new HashMap<>();
	private Map<GradoopId, Vertex> vertexMap;
	private Map<Vertex, List<Edge>> edgesOutMap;
	private LinkedList<GraphSet> paths;
	private ExecutionEnvironment env;
	private Map<Vertex, GraphSet> longestPathMap;

	/**
	 * Constructor
	 * 
	 * @param args:
	 *            input path to EPGM
	 */
	public Metabolism(String[] args) {

		Preconditions.checkArgument(args.length >= 1, "input dir required");
		inputDir = args[0];
		env = ExecutionEnvironment.getExecutionEnvironment();
		this.graph = readLogicalGraph(inputDir);

	}

	/**
	 * graph getter
	 * 
	 * @return the logical graph representing the input data
	 */
	public LogicalGraph getGraph() {
		return graph;
	}

	/**
	 * find all graphs with label {@link #SUBSYSTEM_LABEL} and write them as a
	 * new graph to file in the folder {@link #ALL_SUBSYSTEMS}
	 * 
	 * @throws Exception
	 */
	public void getSubsystems() throws Exception {

		GraphCollection graphCollection = getGraphCollection();

		GraphCollection filtered = graphCollection.select(g -> g.getLabel().equals(SUBSYSTEM_LABEL));

		filtered.writeTo(getJDataSink(ALL_SUBSYSTEMS));

		env.execute();

	}

	/**
	 * get GraphCollection from input path
	 * 
	 * @return all graphs read in
	 */
	private GraphCollection getGraphCollection() {
		String graphHeadFile = inputDir + "/graphs.json";
		String vertexFile = inputDir + "/vertices.json";
		String edgeFile = inputDir + "/edges.json";

		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

		// writeLogicalGraph(graph, inputDir + "/graphs");
		JSONDataSource dataSource = new JSONDataSource(graphHeadFile, vertexFile, edgeFile, config);

		// read graph collection from DataSource
		GraphCollection graphCollection = dataSource.getGraphCollection();
		return graphCollection;
	}

	/**
	 * find a long path (max of all subpaths) in a logical graph and write it as
	 * logical graph to file
	 * 
	 * @throws Exception
	 */
	public void getLongestPath() throws Exception {

		List<Vertex> vertices = graph.getVertices().collect();
		longestPathMap = new HashMap<>(vertices.size());

		setVertexMaps(vertices);
		Set<Vertex> sourceSet = getSources(graph, vertices, "metabolite");

		paths = new LinkedList<>();
		for (Vertex v : sourceSet) {
			GraphSet graphSet = getTargetSet(v, Collections.emptyList());
			paths.add(graphSet);
		}
		GraphSet gs = paths.stream().max((gs1, gs2) -> Integer.compare(gs1.getVertexCount(), gs2.getVertexCount()))
				.orElse(null);

		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
		LogicalGraph lg = LogicalGraph.fromCollections(new GraphHead(GradoopId.get(), "longestPath", null),
				gs.getVertices(), gs.getEdges(), config);

		lg.writeTo(getJDataSink("longestPath"));
		env.execute();
	}

	/**
	 * find all vertices from {@code graph} without incoming edges of type
	 * {@code type}
	 * 
	 * @param graph
	 *            graph, where incoming edges are collected
	 * @param vertices
	 *            vertex list, that should be searched in
	 * @param type
	 *            node type
	 * @return set of nodes without incoming edges
	 * @throws Exception
	 */
	private Set<Vertex> getSources(LogicalGraph graph, List<Vertex> vertices, String type) throws Exception {
		Set<Vertex> sourceSet = new HashSet<>();
		for (Vertex v : vertices) {
			if (v.getPropertyValue("type").toString().equals(type)) {
				List<Edge> incomingEdges = graph.getIncomingEdges(v.getId()).collect();
				if (incomingEdges.isEmpty()) sourceSet.add(v);
			}
		}
		return sourceSet;
	}

	/**
	 * find all vertices without outgoing edges of type "type"
	 * 
	 * @param graph
	 *            graph, where outgoing edges are collected
	 * @param vertices
	 *            vertex list, that should be searched in
	 * @param type
	 *            node type
	 * @return set of nodes without outgoing edges
	 * @throws Exception
	 */
	private Set<Vertex> getSinks(LogicalGraph graph, List<Vertex> vertices, String type) throws Exception {
		Set<Vertex> sinkSet = new HashSet<>();
		int edgecount = 0;
		for (Vertex v : vertices) {
			if (v.getPropertyValue("type").toString().equals(type)) {
				edgecount = graph.getOutgoingEdges(v.getId()).collect().size();
				if (edgecount == 0) sinkSet.add(v);
			}
		}
		return sinkSet;
	}

	/**
	 * fill hashmaps {@link vertexMap} (id -> vertex) and {@link edgesOutMap}
	 * (vertex -> outgoing edges) for graph traversing, edges just store ids of
	 * source and target nodes
	 * 
	 * @param vertices
	 *            values for {@link vertexMap} and keys for {@link edgesOutMap}
	 * @throws Exception
	 */
	private void setVertexMaps(List<Vertex> vertices) throws Exception {
		vertexMap = new HashMap<>(vertices.size());
		edgesOutMap = new HashMap<>(vertices.size());
		for (Vertex v : vertices) {
			vertexMap.put(v.getId(), v);
			edgesOutMap.put(v, graph.getOutgoingEdges(v.getId()).collect());
		}
		// edgesOutMap.values().forEach(list -> list.sort((e1, e2) ->
		// e1.getId().compareTo(e2.getId())));
	}

	/**
	 * creates a new {@link JSONDataSink} to write a logical graph to file
	 * 
	 * @param folder:
	 *            target folder name
	 * @return
	 * @throws IOException
	 */
	private JSONDataSink getJDataSink(String folder) throws IOException {

		File f = new File(inputDir + "/" + folder);
		if (f.exists()) FileUtils.deleteDirectory(f);

		String graphHeadFile = inputDir + "/" + folder + "/graphs.json";
		String vertexFile = inputDir + "/" + folder + "/vertices.json";
		String edgeFile = inputDir + "/" + folder + "/edges.json";

		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

		return new JSONDataSink(graphHeadFile, vertexFile, edgeFile, config);
	}

	/**
	 * prints all metabolites which are only connected (as input) to
	 * {@code extracellular_space} reactions (transport to outside) and String
	 * to print them
	 * 
	 * @param path
	 *            pointing to the {@code extracellular_space} folder
	 * @return the result text
	 * @throws Exception
	 */
	public String findOutputMetabolites(String path) throws Exception {
		LogicalGraph extracellular = readLogicalGraph(path);
		Set<Vertex> sinks = getSinks(extracellular, extracellular.getVertices().collect(), "reaction_blank");
		List<Vertex> allvertices = graph.getVertices().collect();
		setVertexMaps(allvertices);
		List<Vertex> outputs = new LinkedList<>();
		StringBuilder result = new StringBuilder();
		// result.append("Sinksize: " + sinks.size() + "\n");
		for (Vertex v : allvertices) {
			if (v.getPropertyValue("type").toString().equals("metabolite")) {
				boolean contains = true;
				List<Edge> edgesOut = graph.getOutgoingEdges(v.getId()).collect();
				for (Edge edge : edgesOut) {
					if (!sinks.contains(vertexMap.get(edge.getTargetId()))) contains = false;

				}
				if (contains) outputs.add(v);
			}
		}
		for (Vertex out : outputs)
			result.append(out.getLabel() + "\n");
		return result.toString();
	}

	/**
	 * find all vertices of type {@code metabolite} which cannot be created from
	 * input from outside and print it
	 * 
	 * @param path:
	 *            pointing to the {@code extracellular_space} folder
	 * @return the result text
	 * @throws Exception
	 */
	public String findInputMetabolites(String path) throws Exception {
		LogicalGraph extracellular = readLogicalGraph(path);
		Set<Vertex> sources = getSources(extracellular, extracellular.getVertices().collect(), "reaction_blank");
		StringBuilder result = new StringBuilder();
		List<Vertex> allvertices = graph.getVertices().collect();
		setVertexMaps(allvertices);
		GraphSet graphSet = new GraphSet();

		for (Vertex source : sources) {
			Vertex start = vertexMap.get(source.getId());
			graphSet.addVertex(start);
			graphSet = getSubgraphSet(start, graphSet);
		}
		for (Vertex v : allvertices) {
			if (v.getPropertyValue("type").toString().equals("metabolite")) {
				if (!graphSet.containsVertex(v)) {
					result.append(v.getLabel() + "\n");
				}
			}

		}
		return result.toString();
	}

	/**
	 * find connected subgraph by source vertex
	 * 
	 * @param vertex:
	 *            source
	 * @param graphSet:
	 *            collection of subgraph vertices and edges
	 * @return subgraph
	 */
	private GraphSet getSubgraphSet(Vertex vertex, GraphSet graphSet) {

		List<Edge> out = edgesOutMap.get(vertexMap.get(vertex.getId()));
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
	 * finds longest path collections (vertices and edges) by source vertex
	 * without circles
	 * 
	 * @param newVertex
	 *            current source vertex to find longest path
	 * @param alreadyVisited
	 *            list of vertices that already belong to path (stop criterion)
	 * @return longest path collection
	 */
	private GraphSet getTargetSet(Vertex newVertex, List<Vertex> alreadyVisited) {
		List<Edge> out = edgesOutMap.get(newVertex);
		List<Vertex> alreadyVisitedFromHere = copyList(alreadyVisited);
		alreadyVisitedFromHere.add(newVertex);
		Edge newEdge = null;
		GraphSet toReturn = new GraphSet();
		for (Edge edge : out) {
			GraphSet graphSetPart = new GraphSet();
			Vertex target = vertexMap.get(edge.getTargetId());
			if (!alreadyVisitedFromHere.contains(target)) {
				if (!longestPathMap.containsKey(target)) {
					graphSetPart = getTargetSet(target, alreadyVisitedFromHere);
					longestPathMap.put(target, graphSetPart);
				} else graphSetPart = longestPathMap.get(target);
			}
			if (graphSetPart.getVertexCount() > toReturn.getVertexCount()) {
				toReturn = graphSetPart.copy();
				newEdge = edge;
			}
		}
		if (newEdge != null) toReturn.addEdge(newEdge);
		toReturn.addVertex(newVertex);
		return toReturn;

	}

	/**
	 * writes all logical graphs of type subsystem to separate file
	 * 
	 * @param type
	 *            graph property (reaction, subsystem or compartment)
	 * @throws Exception
	 */
	public void writeSubsystems2File(String type) throws Exception {

		GraphCollection graphCollection = getGraphCollection();
		List<GraphHead> graphHeads = graphCollection.getGraphHeads().collect();

		for (GraphHead gh : graphHeads) {
			if (gh.getLabel().equals(type)) {
				LogicalGraph subgraph = graphCollection.getGraph(gh.getId());
				String subsystemName = gh.getPropertyValue("name").toString().trim().replaceAll(" |,|:|;|\\/", "_");

				subgraph.writeTo(getJDataSink(type + "/" + subsystemName));

				// execute program
				env.execute();
			}
		}
	}

	/**
	 * Uses pattern matching to search for all the logical graphs of the active
	 * transport reactions between the compartments and writing the results to a
	 * separate file
	 * 
	 * @throws Exception
	 */

	public void getActiveTransportReactions() throws Exception {
		GraphCollection graphCollection = getGraphCollection();
		GradoopIdList gradoopIdList = new GradoopIdList();

		for (Vertex v : graph
				.match("(m1 {type : \"metabolite\"})-->(r1)-->(m1), (m2 {type : \"metabolite\"})-->(r1)-->(m3 {type : \"metabolite\"})")
				.difference(graph.match("(n1)-->(r1)-->(n1), (n2)-->(r1)-->(n2)," + "(n3)-->(r1)-->(n3)")).getVertices()
				.collect()) {
			if (v.getPropertyValue("type").toString().equals("reaction_blank")) {
				for (GradoopId graphID : v.getGraphIds()) {
					DataSet<GraphHead> graphHead = graphCollection.getGraph(graphID).getGraphHead();
					if (graphHead.count() > 0) {
						if (graphHead.collect().get(0).getLabel().equals("reaction")) {
							gradoopIdList.add(graphID);
						}
					}
				}
			}
		}

		GraphCollection collectionOut = graphCollection.getGraphs(gradoopIdList);
		collectionOut.writeTo(getJDataSink("ActivTransportReactionCollection"));
		env.execute();
	}

	/**
	 * Searches all logical graphs of transport reactions between the
	 * compartments and writing the results to separate file
	 * 
	 * @throws Exception
	 */
	public void getTransportReactions() throws Exception {
		LogicalGraph extracellular = null, cytosol = null;
		GraphCollection graphCollection = getGraphCollection();
		List<GraphHead> graphHeads = graphCollection.getGraphHeads().collect();
		for (GraphHead gh : graphHeads) {
			// The test data only contain 2 different compartments (cytosol &
			// extracellular space)
			if (gh.getLabel().equals("compartment")) {
				switch (gh.getPropertyValue("name").toString()) {
				case "cytosol":
					cytosol = graphCollection.getGraph(gh.getId());
					break;

				default:
					extracellular = graphCollection.getGraph(gh.getId());
					break;
				}
			}
		}

		// All reactions occurring in more than one compartment are transport
		// reactions
		DataSet<Vertex> transportReactions = cytosol.overlap(extracellular).match("({type : \"reaction_blank\"})")
				.getVertices();
		GradoopIdList gradoopIdList = new GradoopIdList();

		// Using the GraphIDs of the reaction_blank nodes to get the logic
		// graphs of the reactions as result
		for (Vertex transportBlancNode : transportReactions.collect()) {
			for (GradoopId graphID : transportBlancNode.getGraphIds()) {
				if (graphCollection.getGraph(graphID).getGraphHead().count() > 0) if (graphCollection.getGraph(graphID)
						.getGraphHead().collect().get(0).getLabel().equals("reaction")) {
					gradoopIdList.add(graphID);
					continue;
				}
			}
		}

		GraphCollection collectionOut = graphCollection.getGraphs(gradoopIdList);
		collectionOut.writeTo(getJDataSink("TransportReactionCollection"));
		env.execute();
	}

	/**
	 * Returns the number of vertices in graph by adding the property
	 * {@code VertexCount}
	 * 
	 * @return VertexCount
	 * @throws Exception
	 */
	public PropertyValue getVertexCount() throws Exception {
		VertexCount vertexCount = new VertexCount();
		if (!hasProperty(vertexCount)) graph = graph.aggregate(vertexCount);
		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		return graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey());

	}

	/**
	 * Returns the number of edges in graph by adding the property
	 * {@code EdgeCount}
	 * 
	 * @return EdgeCount
	 * @throws Exception
	 */
	public PropertyValue getEdgeCount() throws Exception {
		EdgeCount edgeCount = new EdgeCount();
		if (!hasProperty(edgeCount)) graph = graph.aggregate(edgeCount);
		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		return graphHead.getPropertyValue(edgeCount.getAggregatePropertyKey());
	}

	/**
	 * Checks whether the {@link #graph} has a given {@code Count} property set
	 * or not.
	 * 
	 * @param count
	 *            to be checked
	 * @return {@code true} if the property is set, otherwise {@code false}
	 * @throws Exception
	 */
	private boolean hasProperty(Count count) throws Exception {
		return graph.getGraphHead().collect().get(0).getProperties().containsKey(count.getAggregatePropertyKey());
	}

	/**
	 * Adds VertexCount and EdgeCount to graph properties and print counts of
	 * incoming and outgoing Edges for each vertex if they are bigger than their
	 * given thresholds {@code in} and {@code out}. Fills the global hash maps
	 * {@link #edgesInCounts} and {@link #edgesOutCounts}
	 * 
	 * @param in:
	 *            threshold for incoming edges
	 * @param out:
	 *            threshold for outgoing edges
	 * @return
	 * @throws Exception
	 */
	public String getVertexEdges(int in, int out) throws Exception {
		// getEdgeCount();
		// getVertexCount();
		List<Edge> edges = graph.getEdges().collect();
		List<Vertex> vertices = graph.getVertices().collect();
		StringBuilder result = new StringBuilder();
		edgesInCounts = new HashMap<>();
		edgesOutCounts = new HashMap<>();
		int cntIn, cntOut;
		for (Vertex vertex : vertices) {
			GradoopId vertexID = vertex.getId();
			cntIn = 0;
			cntOut = 0;
			for (Edge edge : edges) {
				if (edge.getSourceId().equals(vertexID)) cntOut++;
				if (edge.getTargetId().equals(vertexID)) cntIn++;
			}
			edgesInCounts.put(vertex, cntIn);
			edgesOutCounts.put(vertex, cntOut);
			if (cntIn > in || cntOut > out) result.append("Vertex: " + vertex.getLabel() + ":\t incoming Edges: "
					+ cntIn + "\t outgoing Edges: " + cntOut + "\n");
		}
		return result.toString();
	}

	/**
	 * groups the graph by the clusterId property (subsystems)
	 * 
	 * @throws Exception
	 */
	public void grouping() throws Exception {

		List<String> keys = new LinkedList<>();
		keys.add(CLUSTER_ID_KEY);
		LogicalGraph grouped = graph.groupBy(keys);

		LogicalGraph transformed = grouped.transformVertices((current, transform) -> {
			current.setLabel(current.getPropertyValue(CLUSTER_ID_KEY).toString());
			return current;
		});

		File f = new File(inputDir + "/GroupedLabeledGraph");
		if (f.exists()) {
			FileUtils.deleteDirectory(f);
		}
		writeLogicalGraph(transformed, inputDir + "/GroupedLabeledGraph");

	}

	/**
	 * Searches for subgraphes which appear regularly in the graph collection
	 * and writes them into a file
	 * 
	 * @param sim:
	 *            similarity factor
	 * @throws Exception
	 */
	public void frequentSubgraphMining(float sim) throws Exception {
		GraphCollection graphCollection = getGraphCollection();
		graphCollection = graphCollection.select(g -> g.getLabel().equals(SUBSYSTEM_LABEL));
		GraphCollection frequentPatterns = graphCollection.callForCollection(new TransactionalFSM(sim));
		frequentPatterns.writeTo(getJDataSink("FSM"));
		env.execute();
	}

	/**
	 * Collects all subgraphs matching a pattern and writes them into a file
	 * 
	 * @param pattern:
	 *            pattern matching statement
	 * @param folder:
	 *            name of new folder to save output graph
	 * 
	 * @throws Exception
	 */
	public void writeMatchingPatterns(String pattern, String folder) throws Exception {
		GraphCollection patternGraph = graph.match(pattern);
		writeGraphCollection(patternGraph, inputDir + "/" + folder);
		env.execute();
	}

	/**
	 * Creates another list containing the elements of the given list
	 * 
	 * @param toCopy
	 *            the list whose elements will be put into the new list
	 * @return the new list containing the elements of the given list
	 */
	private <T> List<T> copyList(List<T> toCopy) {
		List<T> toReturn = new LinkedList<>();
		toReturn.addAll(toCopy);
		return toReturn;
	}

}
