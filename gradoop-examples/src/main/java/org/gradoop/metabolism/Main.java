package org.gradoop.metabolism;

public class Main {

	public static void main(String[] args) throws Exception {
		Metabolism mtb = new Metabolism(args);
		mtb.getMembraneReactionGraph();
//		String vcnt = mtb.getVertexCount();
//		String ecnt = mtb.getEdgeCount();
//
//		mtb.getVertexEdges(0, 0);
//
//		System.out.println("VertexCount: " + vcnt);
//		System.out.println("EdgeCount: " + ecnt);

		// mtb.getSubsystems();
		
		// mtb.writeSubsystems2File();
		mtb.getLogestPath();

	}

}
