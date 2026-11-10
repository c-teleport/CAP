using System;
using System.Collections.Generic;
using System.Linq;
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

public class SubscribeExecutorLeaseTests
{
    private readonly IDataStorage _storage = Substitute.For<IDataStorage>();
    private readonly ISubscribeInvoker _invoker = Substitute.For<ISubscribeInvoker>();
    private readonly List<(StatusName State, DateTime? LeasedUntil, int Retries)> _writes = new();

    public SubscribeExecutorLeaseTests()
    {
        _storage.ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), Arg.Any<StatusName>())
            .Returns(call =>
            {
                var m = call.Arg<MediumMessage>();
                _writes.Add((call.Arg<StatusName>(), m.LeasedUntil, m.Retries));
                return Task.CompletedTask;
            });
    }

    [Fact]
    public async Task ExecuteAsync_KeepsTheLease_OnIntermediateFailures_AndDropsItOnTheFinalOne()
    {
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Throws(new InvalidOperationException("boom"));
        var executor = CreateExecutor(failedRetryCount: 3);
        var message = CreateLeasedMessage();

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        // Three in-process attempts: two intermediate Failed writes keep the lease, the final one releases it.
        _writes.Select(w => w.State).Should().AllBeEquivalentTo(StatusName.Failed);
        _writes.Should().HaveCount(3);
        _writes[0].LeasedUntil.Should().NotBeNull();
        _writes[1].LeasedUntil.Should().NotBeNull();
        _writes[2].LeasedUntil.Should().BeNull();
        _writes[2].Retries.Should().Be(3);
    }

    [Fact]
    public async Task ExecuteAsync_DropsTheLease_WhenTheHandlerSucceeds()
    {
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Returns(new ConsumerExecutedResult(null, "1", null, null));
        var executor = CreateExecutor(failedRetryCount: 3);
        var message = CreateLeasedMessage();

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        _writes.Should().ContainSingle().Which.Should().Be((StatusName.Succeeded, (DateTime?)null, 0));
    }

    [Fact]
    public async Task ExecuteAsync_DropsTheLease_WhenNoRetryIsLeft()
    {
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Throws(new InvalidOperationException("boom"));
        var executor = CreateExecutor(failedRetryCount: 1);
        var message = CreateLeasedMessage();

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        _writes.Should().ContainSingle().Which.LeasedUntil.Should().BeNull();
    }

    private ISubscribeExecutor CreateExecutor(int failedRetryCount)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_storage);
        services.AddSingleton(_invoker);
        services.AddCap(x => x.FailedRetryCount = failedRetryCount);
        return services.BuildServiceProvider().GetRequiredService<ISubscribeExecutor>();
    }

    private static ConsumerExecutorDescriptor CreateDescriptor() => new()
    {
        Attribute = new CandidatesTopic("test.topic"),
        ServiceTypeInfo = typeof(NoopSubscriber).GetTypeInfo(),
        ImplTypeInfo = typeof(NoopSubscriber).GetTypeInfo(),
        MethodInfo = typeof(NoopSubscriber).GetMethod(nameof(NoopSubscriber.Handle))!,
        Parameters = new List<ParameterDescriptor>()
    };

    private static MediumMessage CreateLeasedMessage() => new()
    {
        DbId = "1",
        Added = DateTime.Now.AddMinutes(-10),
        StatusName = StatusName.Scheduled,
        LeasedUntil = DateTime.Now.AddMinutes(4),
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
