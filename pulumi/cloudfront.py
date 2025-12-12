"""CloudFront distribution for serving PMTiles."""

import pulumi
import pulumi_aws as aws

from config import name, default_tags, region


def create_origin_access_control() -> aws.cloudfront.OriginAccessControl:
    """Create Origin Access Control for S3 access."""
    oac = aws.cloudfront.OriginAccessControl(
        name("oac"),
        name="osm-h3-tiles-oac",
        description="OAC for OSM-H3 PMTiles S3 bucket",
        origin_access_control_origin_type="s3",
        signing_behavior="always",
        signing_protocol="sigv4",
    )

    return oac


def create_cache_policy() -> aws.cloudfront.CachePolicy:
    """Create cache policy optimized for PMTiles (supports range requests)."""
    cache_policy = aws.cloudfront.CachePolicy(
        name("cache-policy"),
        name="osm-h3-pmtiles-cache-policy",
        comment="Cache policy for PMTiles with range request support",
        default_ttl=86400,  # 1 day
        max_ttl=31536000,  # 1 year
        min_ttl=0,
        parameters_in_cache_key_and_forwarded_to_origin=aws.cloudfront.CachePolicyParametersInCacheKeyAndForwardedToOriginArgs(
            cookies_config=aws.cloudfront.CachePolicyParametersInCacheKeyAndForwardedToOriginCookiesConfigArgs(
                cookie_behavior="none",
            ),
            headers_config=aws.cloudfront.CachePolicyParametersInCacheKeyAndForwardedToOriginHeadersConfigArgs(
                header_behavior="none",
            ),
            query_strings_config=aws.cloudfront.CachePolicyParametersInCacheKeyAndForwardedToOriginQueryStringsConfigArgs(
                query_string_behavior="none",
            ),
            enable_accept_encoding_brotli=True,
            enable_accept_encoding_gzip=True,
        ),
    )

    return cache_policy


def create_origin_request_policy() -> aws.cloudfront.OriginRequestPolicy:
    """Create origin request policy that forwards Range header for PMTiles."""
    policy = aws.cloudfront.OriginRequestPolicy(
        name("origin-request-policy"),
        name="osm-h3-pmtiles-origin-request-policy",
        comment="Origin request policy for PMTiles range requests",
        cookies_config=aws.cloudfront.OriginRequestPolicyCookiesConfigArgs(
            cookie_behavior="none",
        ),
        headers_config=aws.cloudfront.OriginRequestPolicyHeadersConfigArgs(
            header_behavior="whitelist",
            headers=aws.cloudfront.OriginRequestPolicyHeadersConfigHeadersArgs(
                items=["Range", "Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"],
            ),
        ),
        query_strings_config=aws.cloudfront.OriginRequestPolicyQueryStringsConfigArgs(
            query_string_behavior="none",
        ),
    )

    return policy


def create_response_headers_policy() -> aws.cloudfront.ResponseHeadersPolicy:
    """Create response headers policy with CORS support for PMTiles."""
    policy = aws.cloudfront.ResponseHeadersPolicy(
        name("response-headers-policy"),
        name="osm-h3-pmtiles-response-headers-policy",
        comment="Response headers policy with CORS for PMTiles",
        cors_config=aws.cloudfront.ResponseHeadersPolicyCorsConfigArgs(
            access_control_allow_credentials=False,
            access_control_allow_headers=aws.cloudfront.ResponseHeadersPolicyCorsConfigAccessControlAllowHeadersArgs(
                items=["*"],
            ),
            access_control_allow_methods=aws.cloudfront.ResponseHeadersPolicyCorsConfigAccessControlAllowMethodsArgs(
                items=["GET", "HEAD", "OPTIONS"],
            ),
            access_control_allow_origins=aws.cloudfront.ResponseHeadersPolicyCorsConfigAccessControlAllowOriginsArgs(
                items=["*"],
            ),
            access_control_expose_headers=aws.cloudfront.ResponseHeadersPolicyCorsConfigAccessControlExposeHeadersArgs(
                items=["Content-Range", "Accept-Ranges", "Content-Length"],
            ),
            access_control_max_age_sec=86400,
            origin_override=True,
        ),
    )

    return policy


def create_distribution(
    bucket_domain_name: pulumi.Output[str],
    bucket_arn: pulumi.Output[str],
    oac_id: pulumi.Output[str],
    cache_policy_id: pulumi.Output[str],
    origin_request_policy_id: pulumi.Output[str],
    response_headers_policy_id: pulumi.Output[str],
) -> aws.cloudfront.Distribution:
    """Create CloudFront distribution for PMTiles."""
    origin_id = "osm-h3-tiles-s3"

    distribution = aws.cloudfront.Distribution(
        name("distribution"),
        enabled=True,
        comment="OSM-H3 PMTiles distribution",
        default_root_object="",
        price_class="PriceClass_100",  # US, Canada, Europe
        origins=[
            aws.cloudfront.DistributionOriginArgs(
                domain_name=bucket_domain_name,
                origin_id=origin_id,
                origin_path="/tiles",
                origin_access_control_id=oac_id,
            ),
        ],
        default_cache_behavior=aws.cloudfront.DistributionDefaultCacheBehaviorArgs(
            target_origin_id=origin_id,
            viewer_protocol_policy="redirect-to-https",
            allowed_methods=["GET", "HEAD", "OPTIONS"],
            cached_methods=["GET", "HEAD", "OPTIONS"],
            cache_policy_id=cache_policy_id,
            origin_request_policy_id=origin_request_policy_id,
            response_headers_policy_id=response_headers_policy_id,
            compress=True,
        ),
        restrictions=aws.cloudfront.DistributionRestrictionsArgs(
            geo_restriction=aws.cloudfront.DistributionRestrictionsGeoRestrictionArgs(
                restriction_type="none",
            ),
        ),
        viewer_certificate=aws.cloudfront.DistributionViewerCertificateArgs(
            cloudfront_default_certificate=True,
        ),
        tags=default_tags,
    )

    return distribution
