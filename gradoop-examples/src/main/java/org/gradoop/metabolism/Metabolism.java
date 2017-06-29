package org.gradoop.metabolism;

import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;

import com.google.common.base.Preconditions;

public class Metabolism extends AbstractRunner {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Preconditions.checkArgument(
			      args.length == 1, "input dir required");
			    String inputDir  = args[0];
			    System.out.println(args[0]);


			    LogicalGraph graph = readLogicalGraph(inputDir);
			    System.out.println("read graph");
			    
			    System.out.println("Edges: "+graph.getEdges().collect().size());
			    System.out.println("Vertices: "+graph.getVertices().collect().size());

	}

}
