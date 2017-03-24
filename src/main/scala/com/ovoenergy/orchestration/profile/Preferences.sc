import com.ovoenergy.comms.model.Channel.{Email, Phone, Post, SMS}

val customerProfileChannels = List(SMS, Email, Post, Phone)

val triggerChannels = List(SMS, Email)

val customerPreferences = List(Email, SMS)

val cheapestChannel = Email

// Channels in the customer profile, AND trigger ordered by priority
val a = customerProfileChannels
  .filter(triggerChannels.contains)

if(a.isEmpty)
  cheapestChannel
else {
  val result = a.filter(customerPreferences.contains)
}
// Channels in the customer preferences, prior order maintained

