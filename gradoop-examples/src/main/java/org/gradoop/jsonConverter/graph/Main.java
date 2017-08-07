package org.gradoop.jsonConverter.graph;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		String inputdir = args[0];
		String outputdir = args[1];
		JSONParser jparser = new JSONParser();
		JSONObject job = (JSONObject) jparser.parse(new FileReader(inputdir));

		// graphs.json
		// ID Name
		Map<String, LogicGraph> mLogicGraphs = new HashMap<String, LogicGraph>();

		JSONObject jsonCompartments = (JSONObject) job.get("compartments");
		for (Object jsonCompartmentKey : jsonCompartments.keySet()) {
			String strKey = (String) jsonCompartmentKey;
			String strName = (String) jsonCompartments.get(jsonCompartmentKey);
			mLogicGraphs.put(strKey, new LogicGraph("compartment", strName, strKey));
		}

		// nodes.json + edges.json
		Map<String, Node> mMetabolites = new HashMap<String, Node>();

		JSONArray jsonMetabolites = (JSONArray) job.get("metabolites");
		for (Object jsonMetabolite : jsonMetabolites) {
			String strKey = (String) ((JSONObject) jsonMetabolite).get("id");
			if (strKey.contains("_"))
				strKey = strKey.substring(0, strKey.lastIndexOf('_'));
			mMetabolites.put(strKey,
					new Node(strKey, (String) ((JSONObject) jsonMetabolite).get("name"), "metabolite"));
		}

		JSONArray jsonGenes = (JSONArray) job.get("genes");
		Map<String, Node> mGenes = new HashMap<String, Node>();
		for (Object jg : jsonGenes) {

			String strKey = (String) ((JSONObject) jg).get("id");
			mGenes.put(strKey, new Node(strKey, (String) ((JSONObject) jg).get("id"), "gene"));

		}

		List<Node> lReaktions = new ArrayList<Node>();
		List<Edge> lEdges = new ArrayList<Edge>();
		JSONArray jsonReactions = (JSONArray) job.get("reactions");
		for (Object jsonReaktionObject : jsonReactions) {
			JSONObject jsonReaction = (JSONObject) jsonReaktionObject;
			Node nodeReaction = new Node((String) jsonReaction.get("name"), "reaction_blank");
			lReaktions.add(nodeReaction);

			LogicGraph graphReaction = new LogicGraph("reaction", (String) jsonReaction.get("name"), null);
			mLogicGraphs.put((String) jsonReaction.get("name"), graphReaction);
			String strReactionGraphUUID = graphReaction.getStrUUID();

			String strsubsystemGraphUUID = null;
			String subsystem = (String) jsonReaction.get("subsystem");
			if (subsystem != null) {
				if (!mLogicGraphs.containsKey(subsystem)) {
					mLogicGraphs.put(subsystem, new LogicGraph("subsystem", subsystem, null));
				}

				strsubsystemGraphUUID = mLogicGraphs.get(subsystem).getStrUUID();

			}
			if (strsubsystemGraphUUID != null)
				nodeReaction.addGraph(strsubsystemGraphUUID);
			nodeReaction.setClusterID(subsystem);
			nodeReaction.addGraph(strReactionGraphUUID);

			JSONObject metabolite = (JSONObject) jsonReaction.get("metabolites");

			for (Object metaboliteName : metabolite.keySet()) {
				String strMetaboliteName = (String) metaboliteName;
				String strCompartmentID = null;
				if (strMetaboliteName.contains("_")) {
					strCompartmentID = mLogicGraphs
							.get(strMetaboliteName.substring(strMetaboliteName.lastIndexOf('_') + 1)).getStrUUID();
					strMetaboliteName = strMetaboliteName.substring(0, strMetaboliteName.lastIndexOf('_'));
					mMetabolites.get(strMetaboliteName).addGraph(strCompartmentID);
					;
					nodeReaction.addGraph(strCompartmentID);
				}

				Double fCoefficient = (Double) metabolite.get(metaboliteName);

				if (strsubsystemGraphUUID != null)
					mMetabolites.get(strMetaboliteName).addGraph(strsubsystemGraphUUID);
				mMetabolites.get(strMetaboliteName).addGraph(strReactionGraphUUID);
				mMetabolites.get(strMetaboliteName).setClusterID(subsystem);
				String MetaboliteUUID = mMetabolites.get(strMetaboliteName).getStrUUID();

				Edge currentEdge = null;
				if (fCoefficient < 0) {
					currentEdge = new Edge(MetaboliteUUID, nodeReaction.getStrUUID(), fCoefficient, "input");
				} else {
					currentEdge = new Edge(nodeReaction.getStrUUID(), MetaboliteUUID, fCoefficient, "output");
				}

				if (strCompartmentID != null)
					currentEdge.addGraph(strCompartmentID);

				if (strsubsystemGraphUUID != null)
					currentEdge.addGraph(strsubsystemGraphUUID);
				currentEdge.addGraph(strReactionGraphUUID);
				lEdges.add(currentEdge);
			}

			String[] genes = ((String) jsonReaction.get("gene_reaction_rule")).split(" ");
			for (String gene : genes) {
				if (mGenes.containsKey(gene)) {
					if (strsubsystemGraphUUID != null)
						mGenes.get(gene).addGraph(strsubsystemGraphUUID);
					mGenes.get(gene).addGraph(strReactionGraphUUID);
					mGenes.get(gene).setClusterID(subsystem);
					String GeneUUID = mGenes.get(gene).getStrUUID();

					String label = "gene";
					if (Arrays.asList(genes).contains("or")) {
						label = label + "_or";
					}
					if (Arrays.asList(genes).contains("and")) {
						label = label + "_and";
					}
					Edge currentEdge = new Edge(GeneUUID, nodeReaction.getStrUUID(), null, label);
					if (strsubsystemGraphUUID != null)
						currentEdge.addGraph(strsubsystemGraphUUID);
					currentEdge.addGraph(strReactionGraphUUID);
					lEdges.add(currentEdge);
				}

			}

		}

		FileWriter filewriter = new FileWriter(outputdir + "/edges.json");

		for (Edge edge : lEdges) {
			filewriter.write(edge.toJSONString() + '\n');
		}
		System.out.println("edges written to file");
		filewriter.close();

		filewriter = new FileWriter(outputdir + "/graphs.json");
		for (LogicGraph graph : mLogicGraphs.values()) {
			filewriter.write(graph.toJSONString() + '\n');
		}
		System.out.println("graphs written to file");
		filewriter.close();

		filewriter = new FileWriter(outputdir + "/vertices.json");
		for (Node reactionBlancNode : lReaktions) {
			filewriter.write(reactionBlancNode.toJSONString() + '\n');
		}

		for (Node metabolite : mMetabolites.values()) {
			filewriter.write(metabolite.toJSONString() + '\n');
		}

		for (Node gene : mGenes.values()) {
			filewriter.write(gene.toJSONString() + "\n");
		}
		System.out.println("nodes written to file");
		filewriter.close();

	}

}
