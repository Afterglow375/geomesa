package org.locationtech.geomesa.tools.kafka

import org.locationtech.geomesa.tools.common.Runner
import org.locationtech.geomesa.tools.common.commands.{Command, HelpCommand, VersionCommand}
import org.locationtech.geomesa.tools.kafka.commands.{RemoveSchemaCommand, CreateCommand}

object KafkaRunner extends Runner {
  override val scriptName: String = "geomesa-kafka"
  override val commands: List[Command] = List(
    new CreateCommand(jc),
    new HelpCommand(jc),
    new VersionCommand(jc),
    new RemoveSchemaCommand(jc)
  )
}
