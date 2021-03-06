/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.tuples.CSVVertex;

/**
 * Converts an {@link Vertex} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class VertexToCSVVertex extends ElementToCSV<Vertex, CSVVertex> {
  /**
   * Reduce object instantiations.
   */
  private final CSVVertex csvVertex = new CSVVertex();

  @Override
  public CSVVertex map(Vertex vertex) throws Exception {
    csvVertex.setId(vertex.getId().toString());
    csvVertex.setLabel(vertex.getLabel());
    csvVertex.setProperties(getPropertyString(vertex));
    return csvVertex;
  }
}
