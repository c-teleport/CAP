using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace DotNetCore.CAP.Test;

public class SubscribeExecutorShutdownTests
{
    private readonly IDataStorage _storage = Substitute.For<IDataStorage>();
    private readonly ISubscribeInvoker _invoker = Substitute.For<ISubscribeInvoker>();

    private ISubscribeExecutor CreateExecutor()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // Register substitutes before AddCap so its TryAdd registrations do not override them.
        services.AddSingleton(_storage);
        services.AddSingleton(_invoker);
        services.AddCap(_ => { });
        return services.BuildServiceProvider().GetRequiredService<ISubscribeExecutor>();
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotMarkSucceeded_WhenHandlerInterruptedByCancellation()
    {
        // Arrange - the handler observes the token and is cancelled mid-execution.
        using var cts = new CancellationTokenSource();
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>())
            .Throws(_ =>
            {
                cts.Cancel();
                return new OperationCanceledException();
            });

        var executor = CreateExecutor();
        var message = CreateMediumMessage();

        // Act
        Func<Task> act = () => executor.ExecuteAsync(message, CreateDescriptor(), cts.Token);

        // Assert - cancellation propagates and the message is left recoverable (neither Succeeded nor Failed).
        await act.Should().ThrowAsync<OperationCanceledException>();
        await _storage.DidNotReceive().ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), StatusName.Succeeded);
        await _storage.DidNotReceive().ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), StatusName.Failed);
    }

    [Fact]
    public async Task ExecuteAsync_MarksSucceeded_WhenHandlerCompletesNormally()
    {
        // Arrange
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>())
            .Returns(new ConsumerExecutedResult(null, "1", null, null));

        var executor = CreateExecutor();
        var message = CreateMediumMessage();

        // Act
        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        // Assert
        await _storage.Received(1).ChangeReceiveStateAsync(message, StatusName.Succeeded);
    }

    private static ConsumerExecutorDescriptor CreateDescriptor() => new()
    {
        Attribute = new CandidatesTopic("test.topic"),
        ServiceTypeInfo = typeof(NoopSubscriber).GetTypeInfo(),
        ImplTypeInfo = typeof(NoopSubscriber).GetTypeInfo(),
        MethodInfo = typeof(NoopSubscriber).GetMethod(nameof(NoopSubscriber.Handle))!,
        Parameters = new List<ParameterDescriptor>()
    };

    private static MediumMessage CreateMediumMessage() => new()
    {
        DbId = "1",
        Added = DateTime.Now,
        Origin = new Message(
            new Dictionary<string, string>
            {
                { Headers.MessageId, "1" },
                { Headers.MessageName, "test.topic" }
            },
            null)
    };

    private class NoopSubscriber : ICapSubscribe
    {
        public void Handle(string message)
        {
        }
    }
}
