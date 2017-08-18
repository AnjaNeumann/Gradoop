package org.gradoop.jsonConverter.graph;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings({ "unchecked", "serial" })

/**
 * data class to represent edges in xml.
 *
 */
public class Edge extends JSONObject {

	JSONArray m_LogicGraphsList = new JSONArray();
	
	final private String id = GradoopId.get().toString();

	public Edge(String source, String target, String label) {
		this(source, target, null, label);

	}

	public Edge(String source, String target, Double quantity, String label) {
		super();

		this.put("id", id);
		this.put("target", target);
		this.put("source", source);

		JSONObject dataObj = new JSONObject();
		if (quantity != null)
			dataObj.put("quantity", quantity);
		this.put("data", dataObj);

		JSONObject metaObj = new JSONObject();
		metaObj.put("label", label);
		metaObj.put("graphs", m_LogicGraphsList);
		this.put("meta", metaObj);
	}

	public void addGraph(String GraphID) {
		if (!m_LogicGraphsList.contains(GraphID))
			m_LogicGraphsList.add(GraphID);
	}

	public String getId() {
		return id;
	}

}