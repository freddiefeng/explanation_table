import java.util.Properties

object ExplanationTableApp {
  def main(args: Array[String]) {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("config.properties"))
    try {
      val builder = properties.getProperty("builderType") match {
        case "NaiveFlashLightETB" => new NaiveFlashLightETB() with Serializable
        case "FanoutFlashLightETB" => new FanoutFlashLightETB() with Serializable
        case "BroadcastFlashLightETB" => new BroadcastFlashLightETB() with Serializable
        case "BroadcastLaserLightETB" => new BroadcastLaserLightETB() with Serializable
        case "AsyncBroadcastFlashLightETB" => new AsyncBroadcastFlashLightETB() with Serializable
        case null => throw new RuntimeException("Type of builder not specified in config.")
        case _ => throw new RuntimeException("Type of builder invalid.")
      }

      builder.buildTable()
    } catch {
      case e: RuntimeException => println(e.getMessage)
    }
  }
}