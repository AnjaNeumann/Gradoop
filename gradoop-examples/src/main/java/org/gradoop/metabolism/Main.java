package org.gradoop.metabolism;

public class Main {

	public static void main(String[] args) throws Exception {
		Metabolism mtb = new Metabolism(args);

		// finds all active transport reactions and write them to file
		mtb.getActivTransportReactions();

		// prints number of incoming and outgoing edges for each vertex
		mtb.getVertexEdges(0, 0);

		// writes all subsystems to one file
		mtb.getSubsystems();

		// writes all graphs with label "reaction" to a separate file in a
		// subfolder reaction
		mtb.writeSubsystems2File("reaction");
		// analog
		mtb.writeSubsystems2File("subsystem");
		mtb.writeSubsystems2File("compartment");

		// groups the graph by subsystems, transform vertex labels and write
		// graph to file
		mtb.grouping();

		// finds a long path (not necessary the longest) in EPGM and write it to
		// file

		mtb.getLogestPath();

		// find all metabolites that cannot be created from input from outside
		// and print them
		mtb.findInputMetabolites(args[0] + "/compartment/extracellular_space");

		// find all "waste"-metabolites: all metabolites that are just input
		// from extracellular transport reactions
		mtb.findOutputMetabolites(args[0] + "/compartment/extracellular_space");

	}

}
