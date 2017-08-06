package gfftospark

object FeatureIdReader {
  type FeatureType = String
  type AttributeKey = String
  type AttributeValue = String

  type FeatureIdReader = PartialFunction[GffLine, Option[AttributeValue]]

  /**
    * Use the single attribute value as the feature ID
    */
  def singleAttribute: FeatureIdReader = {
    case l: GffLine => l.attributes.left.toOption
    case _ => None
  }

  /**
    * Use the attribute with the given key as the feature ID
    */
  def attributeWithKey(key: AttributeKey): FeatureIdReader =
    attributesFromList(key)

  /**
    * Try to find attributes with a key contained in 'featureKeys'
    *
    * @param featureKeys Attribute keys to consider for the feature ID
    * @return
    */
  def attributesFromList(featureKeys: AttributeKey*): FeatureIdReader = {
    case l: GffLine =>
      for {
        attributes <- l.attributes.right.toOption
        (_, id) <- attributes.find {
          case (featureKey, _) => featureKeys.contains(featureKey.toLowerCase)
        }
      } yield id
  }

  /**
    * Use a different strategy for finding the feature ID depending on the feature type
    *
    * @param readers Partial function of feature types to the feature ID read function
    * @return
    */
  def byFeatureType(readers: PartialFunction[FeatureType, FeatureIdReader]): FeatureIdReader = {
    case l: GffLine =>
      for {
        reader <- readers.lift.apply(l.feature.toLowerCase)
        id <- reader(l)
      } yield id
  }
}