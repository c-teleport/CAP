using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DotNetCore.CAP.Test;

public class ImmediateRetryStorageTests
{
    private static readonly TimeSpan Lookback = TimeSpan.FromSeconds(30);

    // Note: InMemoryStorage uses static dictionaries shared across parallel tests, so these tests only assert
    // about their own (uniquely-generated) message ids rather than clearing the shared store.
    private static IDataStorage CreateStorage()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCap(x => x.UseInMemoryStorage());
        return services.BuildServiceProvider().GetRequiredService<IDataStorage>();
    }

    [Fact]
    public async Task GetReceivedMessagesOfNeedRetry_DoesNotReturn_RecentScheduledMessage()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        var result = await storage.GetReceivedMessagesOfNeedRetry(Lookback);

        result.Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    [Fact]
    public async Task ChangeReceiveStateToImmediateRetry_MakesRecentMessageEligibleImmediately()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        // Sanity: a freshly received message is not yet eligible (inside the lookback window).
        (await storage.GetReceivedMessagesOfNeedRetry(Lookback))
            .Select(m => m.DbId).Should().NotContain(message.DbId);

        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });

        // After flagging, it is picked up immediately, bypassing the lookback window.
        (await storage.GetReceivedMessagesOfNeedRetry(Lookback))
            .Select(m => m.DbId).Should().Contain(message.DbId);
    }

    [Fact]
    public async Task ChangeReceiveState_ClearsImmediateRetry_WhenMessageReExecuted()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });
        (await storage.GetReceivedMessagesOfNeedRetry(Lookback))
            .Select(m => m.DbId).Should().Contain(message.DbId);

        // Re-execution transitions the message out of the RetryImmediately state (self-clearing).
        message.ExpiresAt = DateTime.Now.AddHours(1);
        await storage.ChangeReceiveStateAsync(message, StatusName.Succeeded);

        (await storage.GetReceivedMessagesOfNeedRetry(Lookback))
            .Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    private static Message CreateMessage() => new(
        new Dictionary<string, string>
        {
            { Headers.MessageId, "1" },
            { Headers.MessageName, "test.topic" }
        },
        "payload");
}
