package org.gradoop.metabolism;

public class Main {

	public static void main(String[] args) throws Exception {
		Metabolism mtb = new Metabolism(args);
		// mtb.getMembraneReactionGraph();

		// mtb.getVertexEdges(0, 0);

		// mtb.getSubsystems();

		// mtb.writeSubsystems2File();
		// mtb.getLogestPath();
		// mtb.findInputMetabolites(args[0] +
		// "/compartments/extracellular_space");
		// mtb.findOutputMetabolites(args[0] +
		// "/compartments/extracellular_space");
		// mtb.fsm(0.7f);
		// mtb.grouping();
		String pattern = "(n)-->(m)";
		// "(a)-[*]->(a)";

		mtb.patternMatching(pattern, "testcircle");

	}

}
