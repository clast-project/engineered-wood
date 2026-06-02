// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EngineeredWood.IO;
using EngineeredWood.IO.Tests;

namespace EngineeredWood.AWS.Tests;

public class S3SequentialFileTests : SequentialFileTests
{
    private const string EndpointEnv = "S3_ENDPOINT";
    private const string AccessKeyEnv = "S3_ACCESS_KEY_ID";
    private const string SecretKeyEnv = "S3_SECRET_ACCESS_KEY";
    private const string BucketEnv = "S3_TEST_BUCKET";

    private static AmazonS3Config StorageConfig => new()
    {
        ServiceURL = Environment.GetEnvironmentVariable(EndpointEnv),
        ForcePathStyle = true,
    };

    private static AWSCredentials Credentials => new BasicAWSCredentials(
        Environment.GetEnvironmentVariable(AccessKeyEnv),
        Environment.GetEnvironmentVariable(SecretKeyEnv));

    private static string Bucket =>
        Environment.GetEnvironmentVariable(BucketEnv) ?? "sequential-file-tests";

    protected override async Task<(ISequentialFile File, Func<Task<byte[]>> ReadBack, Func<Task> Cleanup)>
        CreateFileAsync(string testId)
    {
        AmazonS3Client client = new(Credentials, StorageConfig);

        // Ensure the test bucket exists
        try
        {
            await client.PutBucketAsync(Bucket);
        }
        catch
        {
            // Bucket may already exist; ignore.
        }

        var key = $"{testId}_{Guid.NewGuid():N}";
        S3SequentialFile file = new(client, Bucket, key);

        async Task<byte[]> ReadBack()
        {
            using GetObjectResponse? response = await client.GetObjectAsync(Bucket, key);
            using MemoryStream stream = new();
            await response.ResponseStream.CopyToAsync(stream);
            return stream.ToArray();
        }

        async Task Cleanup()
        {
            await client.DeleteObjectAsync(Bucket, key);
        }

        return (file, ReadBack, Cleanup);
    }
}
