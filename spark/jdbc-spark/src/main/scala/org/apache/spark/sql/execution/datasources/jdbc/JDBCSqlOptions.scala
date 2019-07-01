package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class JDBCSqlOptions (
                       @transient private val parameters: CaseInsensitiveMap) extends JDBCOptions(parameters) {
  def this(parameters: Map[String, String]) = this(new CaseInsensitiveMap(parameters))

  val selectClause = parameters.getOrElse(JDBCSqlOptions.JDBC_SELECT_CLAUSE, null)

}
object JDBCSqlOptions   {
  private val jdbcOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    jdbcOptionNames += name.toLowerCase
    name
  }
  val JDBC_SELECT_CLAUSE = newOption("selectClause")//查询语句
}
