package corwur.neo4j

import org.neo4j.driver.v1._
import org.neo4j.graphdb.RelationshipType

import scala.collection.JavaConversions.mapAsJavaMap

object Neo4JUtils {
  type NodeId = Long

  def withSession[T](url: String, username: String, password: String)(f: Session => T) = {
    val driver = GraphDatabase.driver(url, AuthTokens.basic(username, password))
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
      driver.close()
    }
  }

  def inTransaction[T](session: Session)(operation: Transaction => T): T = {
    session.writeTransaction(new TransactionWork[T] {
      override def execute(tx: Transaction): T = {
        operation(tx)
      }
    })
  }

  def createNode(label: String, properties: Map[String, String])(implicit tx: Transaction): NodeId = {
    val props = properties.keys
      .map(key => s"$key: {$key}")
      .mkString(", ")

    val query = s"CREATE (n:$label { $props }) RETURN id(n)"

    val javaParams = mapAsJavaMap[String, Object](properties)
    val result = tx.run(new Statement(query, javaParams))
    result.single().get(0).asLong()
  }

  def createRelationship(nodeA: NodeId, nodeB: NodeId, relType: RelationshipType)(implicit tx: Transaction): Unit =
    tx.run(s"MATCH (a),(b) WHERE id(a) = ${nodeA} AND id(b) = ${nodeB} create unique (a)-[r:${relType.name}]->(b) RETURN r")

  def createOrderedRelationships(nodeIds: Seq[NodeId], relType: RelationshipType)(implicit tx: Transaction): Unit =
    createPairs(nodeIds).foreach { case (nodeIdA, nodeIdB) =>
      createRelationship(nodeIdA, nodeIdB, relType)
    }

  def createPairs[T](elements: Seq[T]): Seq[(T, T)] =
    if (elements.nonEmpty) elements.zip(elements.tail) else Seq.empty
}
