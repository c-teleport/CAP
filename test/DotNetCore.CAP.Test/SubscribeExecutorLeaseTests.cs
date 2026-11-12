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
                return Task.FromResult(true);
            });
    }

    [Fact]
    public async Task ExecuteAsync_StopsInProcessRetries_WhenTheFailedWriteWasFencedOut()
    {
        _storage.ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), Arg.Any<StatusName>()).Returns(false);
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Throws(new InvalidOperationException("boom"));
        var executor = CreateExecutor(failedRetryCount: 3);

        await executor.ExecuteAsync(CreateLeasedMessage(), CreateDescriptor(), CancellationToken.None);

        await _invoker.Received(1).InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>());
        await _storage.Received(1).ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), StatusName.Failed);
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

    [Fact]
    public async Task ExecuteAsync_RenewsTheLease_WhileALongHandlerRuns_AndStopsWhenItReturns()
    {
        // Lease 3 s, so the heartbeat fires every second; the handler outlives two of them.
        _storage.TryRenewReceivedMessageLeaseAsync(Arg.Any<MediumMessage>(), Arg.Any<TimeSpan>()).Returns(true);
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>())
            .Returns(async _ => { await Task.Delay(2500); return new ConsumerExecutedResult(null, "1", null, null); });
        var executor = CreateExecutor(failedRetryCount: 3, leaseSeconds: 3);
        var message = CreateLeasedMessage();

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        var renewals = Renewals();
        renewals.Should().BeGreaterThanOrEqualTo(1);
        await _storage.Received().TryRenewReceivedMessageLeaseAsync(message, TimeSpan.FromSeconds(3));
        _writes.Should().ContainSingle().Which.State.Should().Be(StatusName.Succeeded);

        await Task.Delay(1500);
        Renewals().Should().Be(renewals);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotRenew_ForAShortHandler()
    {
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Returns(new ConsumerExecutedResult(null, "1", null, null));
        var executor = CreateExecutor(failedRetryCount: 3, leaseSeconds: 3);

        await executor.ExecuteAsync(CreateLeasedMessage(), CreateDescriptor(), CancellationToken.None);

        Renewals().Should().Be(0);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotRenew_WhenTheMessageHoldsNoLease()
    {
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>())
            .Returns(async _ => { await Task.Delay(1500); return new ConsumerExecutedResult(null, "1", null, null); });
        var executor = CreateExecutor(failedRetryCount: 3, leaseSeconds: 3);
        var message = CreateLeasedMessage();
        message.LeasedUntil = null;

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        Renewals().Should().Be(0);
    }

    [Fact]
    public async Task ExecuteAsync_StopsRenewing_ButFinishesTheHandler_WhenTheLeaseWasLost()
    {
        _storage.TryRenewReceivedMessageLeaseAsync(Arg.Any<MediumMessage>(), Arg.Any<TimeSpan>()).Returns(false);
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>())
            .Returns(async _ => { await Task.Delay(2500); return new ConsumerExecutedResult(null, "1", null, null); });
        var executor = CreateExecutor(failedRetryCount: 3, leaseSeconds: 3);

        await executor.ExecuteAsync(CreateLeasedMessage(), CreateDescriptor(), CancellationToken.None);

        Renewals().Should().Be(1);
        _writes.Should().ContainSingle().Which.State.Should().Be(StatusName.Succeeded);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotRenew_WhileTheResultIsBeingWritten()
    {
        // Lease 3 s, heartbeat every second; the write outlives a heartbeat and must not be fenced out by one.
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Returns(new ConsumerExecutedResult(null, "1", null, null));
        _storage.ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), Arg.Any<StatusName>())
            .Returns(async _ => { await Task.Delay(1500); return true; });
        var executor = CreateExecutor(failedRetryCount: 3, leaseSeconds: 3);

        await executor.ExecuteAsync(CreateLeasedMessage(), CreateDescriptor(), CancellationToken.None);

        Renewals().Should().Be(0);
    }

    [Fact]
    public async Task ExecuteAsync_RenewsBeforeRunning_WhenTheMessageWaitedInTheBuffer()
    {
        _storage.TryRenewReceivedMessageLeaseAsync(Arg.Any<MediumMessage>(), Arg.Any<TimeSpan>()).Returns(true);
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Returns(new ConsumerExecutedResult(null, "1", null, null));
        var executor = CreateExecutor(failedRetryCount: 3);
        var message = CreateLeasedMessage();
        message.LeasedUntil = DateTime.Now.AddSeconds(10);

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        Renewals().Should().Be(1);
        _writes.Should().ContainSingle().Which.State.Should().Be(StatusName.Succeeded);
    }

    [Fact]
    public async Task ExecuteAsync_SkipsTheHandler_WhenTheLeaseWasLostWhileBuffered()
    {
        _storage.TryRenewReceivedMessageLeaseAsync(Arg.Any<MediumMessage>(), Arg.Any<TimeSpan>()).Returns(false);
        var executor = CreateExecutor(failedRetryCount: 3);
        var message = CreateLeasedMessage();
        message.LeasedUntil = DateTime.Now.AddSeconds(-1);

        var result = await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        result.Succeeded.Should().BeFalse();
        await _invoker.DidNotReceive().InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>());
        _writes.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_StillRuns_WhenTheRenewalThrows_ButTheLeaseIsValid()
    {
        _storage.TryRenewReceivedMessageLeaseAsync(Arg.Any<MediumMessage>(), Arg.Any<TimeSpan>()).ThrowsAsync(new TimeoutException());
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Returns(new ConsumerExecutedResult(null, "1", null, null));
        var executor = CreateExecutor(failedRetryCount: 3);
        var message = CreateLeasedMessage();
        message.LeasedUntil = DateTime.Now.AddSeconds(10);

        await executor.ExecuteAsync(message, CreateDescriptor(), CancellationToken.None);

        _writes.Should().ContainSingle().Which.State.Should().Be(StatusName.Succeeded);
    }

    [Fact]
    public async Task ExecuteAsync_KeepsTheLease_WhenTheSucceededWriteThrows()
    {
        _invoker.InvokeAsync(Arg.Any<ConsumerContext>(), Arg.Any<CancellationToken>()).Returns(new ConsumerExecutedResult(null, "1", null, null));
        _storage.ChangeReceiveStateAsync(Arg.Any<MediumMessage>(), StatusName.Succeeded).ThrowsAsync(new TimeoutException());
        var executor = CreateExecutor(failedRetryCount: 5);

        await executor.ExecuteAsync(CreateLeasedMessage(), CreateDescriptor(), CancellationToken.None);

        _writes.First().State.Should().Be(StatusName.Failed);
        _writes.First().LeasedUntil.Should().NotBeNull();
    }

    private int Renewals() => _storage.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IDataStorage.TryRenewReceivedMessageLeaseAsync));

    private ISubscribeExecutor CreateExecutor(int failedRetryCount, int leaseSeconds = 60)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_storage);
        services.AddSingleton(_invoker);
        services.AddCap(x =>
        {
            x.FailedRetryCount = failedRetryCount;
            x.ReceivedMessageLeaseSeconds = leaseSeconds;
        });
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
