namespace DotNetCore.CAP.RabbitMQ;

public sealed record MessagingTopology
{
    public required string QueueName { get; init; }
    public required string QueueBindingExchangeType { get; init; }
    public string? QueueBindingExchangeName { get; init; }
}