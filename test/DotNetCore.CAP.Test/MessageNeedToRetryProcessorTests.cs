using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Processor;
using DotNetCore.CAP.Transport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace DotNetCore.CAP.Test;

public class MessageNeedToRetryProcessorTests
{
    private readonly IDataStorage _storage = Substitute.For<IDataStorage>();
    private readonly IDispatcher _dispatcher = Substitute.For<IDispatcher>();

    [Fact]
    public async Task ProcessAsync_DispatchesOnlyMessagesItCouldLease()
    {
        var leased = CreateMessage("1");
        var takenElsewhere = CreateMessage("2");
        _storage.GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>()).Returns(new[] { leased, takenElsewhere });
        _storage.TryLeaseReceivedMessageAsync(leased, Arg.Any<TimeSpan>()).Returns(true);
        _storage.TryLeaseReceivedMessageAsync(takenElsewhere, Arg.Any<TimeSpan>()).Returns(false);
        _dispatcher.EnqueueToExecute(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>()).Returns(true);

        await RunOnceAsync(() => LeaseAttempts() == 2);

        await _dispatcher.Received(1).EnqueueToExecute(leased, Arg.Any<ConsumerExecutorDescriptor>());
        await _dispatcher.DidNotReceive().EnqueueToExecute(takenElsewhere, Arg.Any<ConsumerExecutorDescriptor>());
        await _storage.DidNotReceive().ChangeReceiveStateToImmediateRetryAsync(Arg.Any<string[]>());
    }

    [Fact]
    public async Task ProcessAsync_LeasesForTheConfiguredLease_NotTheLookbackWindow()
    {
        var message = CreateMessage("1");
        _storage.GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>()).Returns(new[] { message });
        _storage.TryLeaseReceivedMessageAsync(message, Arg.Any<TimeSpan>()).Returns(true);
        _dispatcher.EnqueueToExecute(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>()).Returns(true);

        await RunOnceAsync(() => LeaseAttempts() == 1, leaseSeconds: 75);

        await _storage.Received(1).GetReceivedMessagesOfNeedRetry(TimeSpan.FromSeconds(240));
        await _storage.Received(1).TryLeaseReceivedMessageAsync(message, TimeSpan.FromSeconds(75));
    }

    [Fact]
    public async Task ProcessAsync_FlagsMessageForImmediateRetry_WhenDispatcherRejectsIt()
    {
        var rejected = CreateMessage("2");
        _storage.GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>()).Returns(new[] { rejected });
        _storage.TryLeaseReceivedMessageAsync(rejected, Arg.Any<TimeSpan>()).Returns(true);
        _dispatcher.EnqueueToExecute(rejected, Arg.Any<ConsumerExecutorDescriptor>()).Returns(false);

        await RunOnceAsync(() => Flagged() == 1);

        await _storage.Received(1).ChangeReceiveStateToImmediateRetryAsync(Arg.Is<string[]>(ids => ids.Length == 1 && ids[0] == "2"));
    }

    [Fact]
    public async Task ProcessAsync_DoesNotStartASecondPoll_WhileThePreviousOneIsStillRunning()
    {
        var firstPoll = new TaskCompletionSource<IEnumerable<MediumMessage>>();
        _storage.GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>()).Returns(_ => firstPoll.Task);
        var options = Options.Create(new CapOptions { UseStorageLock = false, FailedRetryInterval = 1 });
        var processor = new MessageNeedToRetryProcessor(options, NullLogger<MessageNeedToRetryProcessor>.Instance, _dispatcher, _storage);
        var provider = new ServiceCollection().AddSingleton(_storage).BuildServiceProvider();
        using var cts = new CancellationTokenSource();
        using var context = new ProcessingContext(provider, cts.Token);

        await processor.ProcessAsync(context);
        await processor.ProcessAsync(context);

        await _storage.Received(1).GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>());
        firstPoll.SetResult(Array.Empty<MediumMessage>());
    }

    private int LeaseAttempts() => _storage.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IDataStorage.TryLeaseReceivedMessageAsync));

    private int Flagged() => _storage.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IDataStorage.ChangeReceiveStateToImmediateRetryAsync));

    // ProcessAsync starts the received-retry loop on a background task and then waits one interval, so the
    // assertions wait for the loop to reach the observed call instead of racing it.
    private async Task RunOnceAsync(Func<bool> done, int leaseSeconds = 60)
    {
        var options = Options.Create(new CapOptions { UseStorageLock = false, FailedRetryInterval = 1, ReceivedMessageLeaseSeconds = leaseSeconds });
        var processor = new MessageNeedToRetryProcessor(options, NullLogger<MessageNeedToRetryProcessor>.Instance, _dispatcher, _storage);
        var provider = new ServiceCollection().AddSingleton(_storage).BuildServiceProvider();
        using var cts = new CancellationTokenSource();
        using var context = new ProcessingContext(provider, cts.Token);

        await processor.ProcessAsync(context);

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5) && !done())
            await Task.Delay(20);
        // Give the loop a moment to finish the iteration after the observed call.
        await Task.Delay(50);
    }

    private static MediumMessage CreateMessage(string id) => new()
    {
        DbId = id,
        Origin = new Message(new Dictionary<string, string> { { Headers.MessageId, id }, { Headers.MessageName, "test.topic" } }, null),
        Added = DateTime.Now.AddHours(-1),
        StatusName = StatusName.RetryImmediately
    };
}
