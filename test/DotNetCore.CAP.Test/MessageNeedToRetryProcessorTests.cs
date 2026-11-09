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
    public async Task ProcessAsync_MarksRetryStarted_BeforeDispatchingEachReceivedMessage()
    {
        var messages = new[] { CreateMessage("1"), CreateMessage("2") };
        _storage.GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>()).Returns(messages);
        _dispatcher.EnqueueToExecute(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor?>()).Returns(true);

        await RunOnceAsync();

        Received.InOrder(() =>
        {
            _storage.ChangeReceiveStateToRetryStartedAsync(messages[0]);
            _dispatcher.EnqueueToExecute(messages[0], Arg.Any<ConsumerExecutorDescriptor?>());
            _storage.ChangeReceiveStateToRetryStartedAsync(messages[1]);
            _dispatcher.EnqueueToExecute(messages[1], Arg.Any<ConsumerExecutorDescriptor?>());
        });
        await _storage.DidNotReceive().ChangeReceiveStateToImmediateRetryAsync(Arg.Any<string[]>());
    }

    [Fact]
    public async Task ProcessAsync_FlagsMessageForImmediateRetry_WhenDispatcherRejectsIt()
    {
        var accepted = CreateMessage("1");
        var rejected = CreateMessage("2");
        _storage.GetReceivedMessagesOfNeedRetry(Arg.Any<TimeSpan>()).Returns(new[] { accepted, rejected });
        _dispatcher.EnqueueToExecute(accepted, Arg.Any<ConsumerExecutorDescriptor?>()).Returns(true);
        _dispatcher.EnqueueToExecute(rejected, Arg.Any<ConsumerExecutorDescriptor?>()).Returns(false);

        await RunOnceAsync();

        await _storage.Received(1).ChangeReceiveStateToImmediateRetryAsync(Arg.Is<string[]>(ids => ids.Length == 1 && ids[0] == "2"));
    }

    private async Task RunOnceAsync()
    {
        var options = Options.Create(new CapOptions { UseStorageLock = false, FailedRetryInterval = 1 });
        var processor = new MessageNeedToRetryProcessor(options, NullLogger<MessageNeedToRetryProcessor>.Instance, _dispatcher, _storage);
        var provider = new ServiceCollection().AddSingleton(_storage).BuildServiceProvider();
        using var cts = new CancellationTokenSource();
        using var context = new ProcessingContext(provider, cts.Token);

        // ProcessAsync starts the received-retry loop on a background task and then waits one interval.
        await processor.ProcessAsync(context);

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5)
               && _dispatcher.ReceivedCalls().Count() < (await _storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Count())
            await Task.Delay(20);
    }

    private static MediumMessage CreateMessage(string id) => new()
    {
        DbId = id,
        Origin = new Message(new Dictionary<string, string> { { Headers.MessageId, id }, { Headers.MessageName, "test.topic" } }, null),
        Added = DateTime.Now.AddHours(-1)
    };
}
