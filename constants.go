package protobus

const (
	// Header/Metadata keys which marks the reason and context why the message was deemed poisoned.
	HeaderDeadLetterReason      = "HeaderDeadLetterReason"
	HeaderDeadLetterStackTrace  = "DeadLetterStackTrace"
	HeaderDeadLetterSourceTopic = "DeadLetterSourceTopic"
	HeaderDeadLetterHandler     = "DeadLetterHandler"
	HeaderDeadLetterSubscriber  = "DeadLetterSubscriber"

	// Header/Metadata for AMQP-RabbitMQ
	//HeaderRabbitMQReplyTo = "amq.rabbitmq.reply-to"

	// Custom Header/Metadata
	HeaderReplyTo = "ProtoBusReplyTo"

	// Custom Context Keys
	ContextRouteId = "ProtoBusRouteId"
)
