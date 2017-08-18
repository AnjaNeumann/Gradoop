package org.gradoop.jsonConverter.graph;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * convertes BiGG Model Data to EPGM needs two arguments: input and output path
 * input path: path to BiGG Model data, output path: folder, where EPGM should
 * be stored
 *
 */
public class Main {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		String inputdir = args[0];
		String outputdir = args[1];
		JSONParser jparser = new JSONParser();

		// read bigg model json file
		JSONObject job = (JSONObject) jparser.parse(new FileReader(inputdir));

		// graphs.json
		// ID Name
		Map<String, LogicGraph> mLogicGraphs = new HashMap<String, LogicGraph>();

		// read all compartments
		JSONObject jsonCompartments = (JSONObject) job.get("compartments");
		for (Object jsonCompartmentKey : jsonCompartments.keySet()) {
			String strKey = (String) jsonCompartmentKey;
			String strName = (String) jsonCompartments.get(jsonCompartmentKey);
			mLogicGraphs.put(strKey, new LogicGraph("compartment", strName, strKey));
		}

		Map<String, Node> mMetabolites = new HashMap<String, Node>();

		// read all metabolites
		JSONArray jsonMetabolites = (JSONArray) job.get("metabolites");
		for (Object jsonMetabolite : jsonMetabolites) {
			String strKey = (String) ((JSONObject) jsonMetabolite).get("id");
			if (strKey.contains("_")) strKey = strKey.substring(0, strKey.lastIndexOf('_'));
			mMetabolites.put(strKey,
					new Node(strKey, (String) ((JSONObject) jsonMetabolite).get("name"), "metabolite"));
		}

		// read all genes
		JSONArray jsonGenes = (JSONArray) job.get("genes");
		Map<String, Node> mGenes = new HashMap<String, Node>();
		for (Object jg : jsonGenes) {
			String strKey = (String) ((JSONObject) jg).get("id");
			mGenes.put(strKey, new Node(strKey, (String) ((JSONObject) jg).get("id"), "gene"));
		}

		List<Node> lReaktions = new ArrayList<Node>();
		List<Edge> lEdges = new ArrayList<Edge>();
		JSONArray jsonReactions = (JSONArray) job.get("reactions");

		// read all reactions
		for (Object jsonReaktionObject : jsonReactions) {
			JSONObject jsonReaction = (JSONObject) jsonReaktionObject;
			Node nodeReaction = new Node((String) jsonReaction.get("name"), "reaction_blank");
			lReaktions.add(nodeReaction);

			// create graph that represent the reaction
			LogicGraph graphReaction = new LogicGraph("reaction", (String) jsonReaction.get("name"), null);
			mLogicGraphs.put((String) jsonReaction.get("name"), graphReaction);

			// id to connect the genes, metabolites and reaction_blanc in
			// reaction graph
			String strReactionGraphUUID = graphReaction.getId();

			String strsubsystemGraphUUID = null;

			// get subsystem of reaction
			String subsystem = (String) jsonReaction.get("subsystem");

			// create new subsystem or get id of existing subsystem
			if (subsystem != null) {
				if (!mLogicGraphs.containsKey(subsystem)) {
					mLogicGraphs.put(subsystem, new LogicGraph("subsystem", subsystem, null));
				}
				strsubsystemGraphUUID = mLogicGraphs.get(subsystem).getId();
			}

			// set infomations to blanc_node
			if (strsubsystemGraphUUID != null) nodeReaction.addGraph(strsubsystemGraphUUID);
			nodeReaction.setClusterID(subsystem);
			nodeReaction.addGraph(strReactionGraphUUID);

			// get metabolites of current reactions
			JSONObject metabolite = (JSONObject) jsonReaction.get("metabolites");
			for (Object metaboliteName : metabolite.keySet()) {
				String strMetaboliteName = (String) metaboliteName;
				String strCompartmentID = null;

				// get compartment of metabolite
				if (strMetaboliteName.contains("_")) {
					strCompartmentID = mLogicGraphs
							.get(strMetaboliteName.substring(strMetaboliteName.lastIndexOf('_') + 1)).getId();
					strMetaboliteName = strMetaboliteName.substring(0, strMetaboliteName.lastIndexOf('_'));
					mMetabolites.get(strMetaboliteName).addGraph(strCompartmentID);
					nodeReaction.addGraph(strCompartmentID);
				}

				Double fCoefficient = (Double) metabolite.get(metaboliteName);

				// set informations to metabolite
				if (strsubsystemGraphUUID != null) mMetabolites.get(strMetaboliteName).addGraph(strsubsystemGraphUUID);
				mMetabolites.get(strMetaboliteName).addGraph(strReactionGraphUUID);
				mMetabolites.get(strMetaboliteName).setClusterID(subsystem);

				String MetaboliteUUID = mMetabolites.get(strMetaboliteName).getId();

				// create edges between reaction_blank node and metabolite node
				Edge currentEdge = fCoefficient < 0
						? new Edge(MetaboliteUUID, nodeReaction.getId(), fCoefficient, "input")
						: new Edge(nodeReaction.getId(), MetaboliteUUID, fCoefficient, "output");

				if (strCompartmentID != null) currentEdge.addGraph(strCompartmentID);

				if (strsubsystemGraphUUID != null) currentEdge.addGraph(strsubsystemGraphUUID);
				currentEdge.addGraph(strReactionGraphUUID);
				lEdges.add(currentEdge);
			}

			// get all genes of reaction
			String[] genes = ((String) jsonReaction.get("gene_reaction_rule")).split(" ");
			for (String gene : genes) {
				if (mGenes.containsKey(gene)) {
					if (strsubsystemGraphUUID != null) mGenes.get(gene).addGraph(strsubsystemGraphUUID);
					mGenes.get(gene).addGraph(strReactionGraphUUID);
					mGenes.get(gene).setClusterID(subsystem);
					String GeneUUID = mGenes.get(gene).getId();

					String label = "gene";
					if (Arrays.asList(genes).contains("or")) label = label + "_or";
					if (Arrays.asList(genes).contains("and")) label = label + "_and";
					Edge currentEdge = new Edge(GeneUUID, nodeReaction.getId(), null, label);
					if (strsubsystemGraphUUID != null) currentEdge.addGraph(strsubsystemGraphUUID);
					currentEdge.addGraph(strReactionGraphUUID);
					lEdges.add(currentEdge);
				}

			}

		}

		write(lEdges, outputdir + "/edges.json");
		System.out.println("edges written to file");

		write(mLogicGraphs.values(), outputdir + "/graphs.json");
		System.out.println("graphs written to file");

		List<Node> nodes = new ArrayList<>(lReaktions.size() + mMetabolites.size() + mGenes.size());
		nodes.addAll(lReaktions);
		nodes.addAll(mMetabolites.values());
		nodes.addAll(mGenes.values());
		write(nodes, outputdir + "/vertices.json");
		System.out.println("nodes written to file");
	}

	private static void write(Collection<? extends JSONObject> toWrite, String filename) throws IOException {
		try (FileWriter filewriter = new FileWriter(filename)) {
			for (JSONObject object : toWrite)
				filewriter.write(object.toJSONString() + "\n");
		}
	}
}
