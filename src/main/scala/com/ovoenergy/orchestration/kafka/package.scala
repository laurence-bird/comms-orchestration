package com.ovoenergy.orchestration

package object kafka {

  case class KafkaConfig(hosts: String, groupId: String, topic: String)

}
