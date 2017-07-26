package org.gradoop.metabolism;

public class Main {

	public static void main(String[] args) throws Exception {
		Metabolism mtb = new Metabolism(args);
		// mtb.getMembraneReactionGraph();

		// mtb.getVertexEdges(0, 0);

		// mtb.getSubsystems();

		// mtb.writeSubsystems2File();
		mtb.getLogestPath();
		// mtb.fsm();
		// mtb.grouping();

	}

}
