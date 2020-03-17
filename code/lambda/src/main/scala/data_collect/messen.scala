package data_collect

import java.util.Properties
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import net.liftweb.json.Serialization.write

//Define messen class
case class messen(tweet_id:String,
                  Messen_id:String,
                  User_id: String,
                  Generator_id: String,
                  C_O2: Float,
                  C_H2S: Float,
                  T_H2O: Float,
                  T_Machine: Float,
                  T_Evironment: Float,
                  P_Oil: Float,
                  P_Tank: Float,
                  U_AB: Float,
                  I_A: Float,
                  I_B: Float,
                  I_c: Float,
                  Veclovity: Float,
                  Momen: Float,
                  Type: Int,
                  Created: Int
                )

object messen {

}
