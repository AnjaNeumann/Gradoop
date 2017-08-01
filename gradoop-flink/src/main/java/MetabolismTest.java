
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.junit.Test;

public class MetabolismTest extends GradoopFlinkTestBase {

	@Test
	public void mTest() throws Exception {

		/*
		 * LogicalGraph graph2 = readLogicalGraph(inputDir); graph2
		 * .aggregate(vertexCount) .aggregate(edgeCount);
		 * 
		 * EPGMGraphHead graphHead = graph2.getGraphHead().collect().get(0);
		 */
		// LogicalGraph graph = readLogicalGraph(inputDir);
		LogicalGraph graph = getLoaderFromString("[()-->()<--()]").getDatabase().getDatabaseGraph();
		VertexCount vertexCount = new VertexCount();
		EdgeCount edgeCount = new EdgeCount();
		graph = graph.aggregate(vertexCount).aggregate(edgeCount);

		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		String cnt = graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey()).getString();
		// List<GraphHead> collected = graph.getGraphHead().collect();
		// collected.get(0).getPropertyValue(vertexCount.getAggregatePropertyKey());
		System.out.println(cnt);

		assertTrue("vertex count not set", graphHead.hasProperty(vertexCount.getAggregatePropertyKey()));
		assertTrue("edge count not set", graphHead.hasProperty(edgeCount.getAggregatePropertyKey()));

		assertCounts(graphHead, 3L, 2L);
	}

	@Test
	public void testSingleGraphVertexAndEdgeCount() throws Exception {
		LogicalGraph graph = getLoaderFromString("[()-->()<--()]").getDatabase().getDatabaseGraph();

		VertexCount vertexCount = new VertexCount();
		EdgeCount edgeCount = new EdgeCount();

		graph = graph.aggregate(vertexCount).aggregate(edgeCount);

		EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);
		graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey());
		assertTrue("vertex count not set", graphHead.hasProperty(vertexCount.getAggregatePropertyKey()));
		assertTrue("edge count not set", graphHead.hasProperty(edgeCount.getAggregatePropertyKey()));

		// assertCounts(graphHead, 3L, 2L);
	}

	private void assertCounts(EPGMGraphHead graphHead, long expectedVertexCount, long expectedEdgeCount) {

		assertEquals("wrong vertex count", expectedVertexCount,
				graphHead.getPropertyValue(new VertexCount().getAggregatePropertyKey()).getLong());
		assertEquals("wrong edge count", expectedEdgeCount,
				graphHead.getPropertyValue(new EdgeCount().getAggregatePropertyKey()).getLong());
	}

}
