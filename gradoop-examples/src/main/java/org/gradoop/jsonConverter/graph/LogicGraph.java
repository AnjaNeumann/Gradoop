package org.gradoop.jsonConverter.graph;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
/**
 * data class to represent graphs as JSONObjects.
 *
 */
public class LogicGraph extends JSONObject {

	final private String id = GradoopId.get().toString();
	final private String name;

	/**
	 * constructor
	 * 
	 * @param strLable
	 *            graph label
	 * @param strName
	 *            graph name
	 * @param strOldID
	 *            oldID of graph
	 */
	@SuppressWarnings("unchecked")
	public LogicGraph(String strLable, String strName, String strOldID) {
		this.name = strName;
		this.put("id", id);

		JSONObject data = new JSONObject();
		data.put("name", strName);

		if (strOldID != null) data.put("id", strOldID);

		this.put("data", data);

		JSONObject meta = new JSONObject();
		meta.put("label", strLable);
		this.put("meta", meta);
	}

	/**
	 * 
	 * @return {@link id}
	 */
	public String getId() {
		return id;
	}

	/**
	 * return graph name
	 * 
	 * @return {@link name}
	 */
	public String getName() {
		return name;
	}

}
