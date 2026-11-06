using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Transport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace DotNetCore.CAP.Test;

public class ConsumerRegisterShutdownTests
{
    [Fact]
    public async Task ReStartAsync_DoesNotRecreateConsumers_AfterStopReceiving()
    {
        // Arrange - a client whose StopReceivingAsync reports ConsumerCancelled, as the RabbitMQ client does on basic.cancel-ok.
        var client = Substitute.For<IConsumerClient>();
        client.StopReceivingAsync().Returns(_ =>
        {
            client.OnLogCallback!(new LogMessageEventArgs { LogType = MqLogType.ConsumerCancelled, Reason = "tag" });
            return Task.CompletedTask;
        });
        client.ListeningAsync(Arg.Any<System.TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(async x => await Task.Delay(Timeout.Infinite, x.Arg<CancellationToken>()));

        var factory = Substitute.For<IConsumerClientFactory>();
        factory.CreateAsync(Arg.Any<string>(), Arg.Any<byte>()).Returns(client);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(factory);
        services.AddSingleton(Substitute.For<ITransport>());
        services.AddCap(x =>
        {
            x.ConsumerThreadCount = 1;
            x.UseInMemoryStorage();
        });
        services.AddTransient<Subscriber>();
        var provider = services.BuildServiceProvider();

        var register = new ConsumerRegister(NullLogger<ConsumerRegister>.Instance, provider);
        using var cts = new CancellationTokenSource();
        await register.StartAsync(cts.Token);
        await WaitForClientsAsync(factory, 2);

        // Act
        await register.StopReceivingAsync();
        register.IsHealthy().Should().BeFalse();
        await register.ReStartAsync();

        // Assert - no new consumer created; only the topic-fetch client and the single listening client exist.
        await factory.Received(2).CreateAsync(Arg.Any<string>(), Arg.Any<byte>());

        await cts.CancelAsync();
    }

    private static async Task WaitForClientsAsync(IConsumerClientFactory factory, int count)
    {
        for (var i = 0; i < 100 && factory.ReceivedCalls().Count() < count; i++)
        {
            await Task.Delay(20);
        }
    }

    private class Subscriber : ICapSubscribe
    {
        [CapSubscribe("shutdown.topic")]
        public void Handle(string message)
        {
        }
    }
}
