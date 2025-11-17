using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.RabbitMQ;
using DotNetCore.CAP.Transport;
using NSubstitute;
using RabbitMQ.Client;
using Xunit;

namespace DotNetCore.CAP.Test
{
    public class RabbitMqBasicConsumerTests
    {
        [Fact]
        public async Task HandleBasicDeliverAsync_CopiesSharedBufferBody_Concurrent()
        {
            // Arrange
            RabbitMqBasicConsumer? consumer = null;
            var results = new ConcurrentDictionary<ulong, byte[]>();
            var channelSub = Substitute.For<IChannel>();
            var msgCallback = new Func<TransportMessage, object?, Task>(async (msg, tag) =>
            {
                results[(ulong)tag!] = msg.Body.ToArray();
                await consumer!.BasicAck((ulong)tag);
            });
            var logCallback = new Action<LogMessageEventArgs>(_ => { });
            consumer = new RabbitMqBasicConsumer(
                channelSub,
                concurrent: 1,
                groupName: "test-group",
                msgCallback: msgCallback,
                logCallback: logCallback,
                customHeadersBuilder: null,
                serviceProvider: null
            );
            var propertiesSub = Substitute.For<IReadOnlyBasicProperties>();
            propertiesSub.Headers.Returns(new Dictionary<string, object?>());
            byte[] sharedBuffer = new byte[10];
            for (int i = 0; i < sharedBuffer.Length; i++) sharedBuffer[i] = (byte)i;
            
            var t1 = consumer.HandleBasicDeliverAsync("ctag1", 1, false, "ex", "rk", propertiesSub, sharedBuffer);
            sharedBuffer[5] = 100;
            var t2 = consumer.HandleBasicDeliverAsync("ctag2", 2, false, "ex", "rk", propertiesSub, sharedBuffer);

            await Task.WhenAll(t1, t2);
            Assert.NotEqual(results[1], results[2]);
        }
    }
}
