namespace DotNetCore.CAP.RabbitMQ;

public abstract record MessagingTopology
{
    public required string GroupName { get; init; }
}

public sealed record TopicMessagingTopology : MessagingTopology { }

public sealed record ConsistentHashMessagingTopology : MessagingTopology
{
    public required string QueueName { get; init; }
    public required string QueueBindingExchangeName { get; init; }
}
