package org.gradoop.metabolism;

import static org.junit.Assert.assertTrue;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.junit.Test;

public class MetabolismTest extends AbstractRunner {

	@Test
	public void mTest() throws Exception {

		String inputDir = "src/main/resources/data/json/Metabolism";

		LogicalGraph graph = readLogicalGraph(inputDir);
		VertexCount vertexCount = new VertexCount();
		EdgeCount edgeCount = new EdgeCount();
		graph = graph.aggregate(vertexCount).aggregate(edgeCount);

		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey());

		assertTrue("vertex count not set", graphHead.hasProperty(vertexCount.getAggregatePropertyKey()));
		assertTrue("edge count not set", graphHead.hasProperty(edgeCount.getAggregatePropertyKey()));
	}

}
