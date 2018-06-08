/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2018-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.repository.composer.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobRef;
import org.sonatype.nexus.common.hash.HashAlgorithm;
import org.sonatype.nexus.repository.Repository;
import org.sonatype.nexus.repository.storage.Asset;
import org.sonatype.nexus.repository.storage.Component;
import org.sonatype.nexus.repository.storage.StorageTx;
import org.sonatype.nexus.repository.view.Content;
import org.sonatype.nexus.repository.view.ContentTypes;
import org.sonatype.nexus.repository.view.Payload;
import org.sonatype.nexus.repository.view.payloads.StringPayload;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonMap;
import static org.sonatype.nexus.repository.composer.internal.ComposerPathUtils.buildZipballPath;

/**
 * Class encapsulating JSON processing for Composer-format repositories, including operations for parsing JSON indexes
 * and rewriting them to be compatible with a proxy repository.
 */
@Named
@Singleton
public class ComposerJsonProcessor
{
  private static final String PACKAGE_JSON_PATH = "/p/%package%.json";

  private static final String VENDOR_AND_PROJECT = "%s/%s";

  private static final Map<String,String> PACKAGE_JSON_KEYS;

  private static final String ZIP_TYPE = "zip";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ");

  private ComposerJsonExtractor composerJsonExtractor;

  static {
    Map<String,String> package_json_keys_tmp = new HashMap<>();
    package_json_keys_tmp.put("AUTOLOAD_KEY", "autoload");
    package_json_keys_tmp.put("AUTHORS_KEY", "authos");
    package_json_keys_tmp.put("BIN_KEY", "bin");
    package_json_keys_tmp.put("CONFLICT_KEY", "conflict");
    package_json_keys_tmp.put("DESCRIPTION_KEY", "description");
    package_json_keys_tmp.put("DIST_KEY", "dist");
    package_json_keys_tmp.put("EXTRA_KEY", "extra");
    package_json_keys_tmp.put("KEYWORDS_KEY", "keywords");
    package_json_keys_tmp.put("LICENSE_KEY", "license");
    package_json_keys_tmp.put("NAME_KEY", "name");
    package_json_keys_tmp.put("PACKAGES_KEY", "packages");
    package_json_keys_tmp.put("PACKAGE_NAMES_KEY", "packageNames");
    package_json_keys_tmp.put("PROVIDE_KEY", "provide");
    package_json_keys_tmp.put("PROVIDERS_KEY", "providers");
    package_json_keys_tmp.put("PROVIDERS_URL_KEY", "providers-url");
    package_json_keys_tmp.put("REFERENCE_KEY", "reference");
    package_json_keys_tmp.put("REQUIRE_KEY", "require");
    package_json_keys_tmp.put("REQUIRE_DEV_KEY", "require-dev");
    package_json_keys_tmp.put("SHA256_KEY", "sha256");
    package_json_keys_tmp.put("SHASUM_KEY", "shasum");
    package_json_keys_tmp.put("SOURCE_KEY", "source");
    package_json_keys_tmp.put("SUGGEST_KEY", "suggest");
    package_json_keys_tmp.put("TARGET_DIR_KEY", "target-dir");
    package_json_keys_tmp.put("TIME_KEY", "time");
    package_json_keys_tmp.put("TYPE_KEY", "type");
    package_json_keys_tmp.put("UID_KEY", "uid");
    package_json_keys_tmp.put("URL_KEY", "url");
    package_json_keys_tmp.put("VERSION_KEY", "version");
    PACKAGE_JSON_KEYS = Collections.unmodifiableMap(package_json_keys_tmp);
  }

  @Inject
  public ComposerJsonProcessor(final ComposerJsonExtractor composerJsonExtractor) {
    this.composerJsonExtractor = checkNotNull(composerJsonExtractor);
  }

  /**
   * Generates a packages.json file (inclusive of all projects) based on the list.json provided as a payload. Expected
   * usage is to "go remote" on the current repository to fetch a list.json copy, then pass it to this method to build
   * the packages.json for the client to use.
   */
  public Content generatePackagesFromList(final Repository repository, final Payload payload) throws IOException {
    // TODO: Parse using JSON tokens rather than loading all this into memory, it "should" work but I'd be careful.
    Map<String, Object> listJson = parseJson(payload);
    return buildPackagesJson(repository, new LinkedHashSet<>((Collection<String>) listJson.get(PACKAGE_JSON_KEYS.get("PACKAGE_NAMES_KEY"))));
  }

  /**
   * Generates a packages.json file (inclusive of all projects) based on the components provided. Expected usage is
   * for a hosted repository to be queried for its components, which are then provided to this method to build the
   * packages.json for the client to use.
   */
  public Content generatePackagesFromComponents(final Repository repository, final Iterable<Component> components)
      throws IOException
  {
    return buildPackagesJson(repository, StreamSupport.stream(components.spliterator(), false)
        .map(component -> component.group() + "/" + component.name()).collect(Collectors.toSet()));
  }

  /**
   * Builds a packages.json file as a {@code Content} instance containing the actual JSON for the given providers.
   */
  private Content buildPackagesJson(final Repository repository, final Set<String> names) throws IOException {
    Map<String, Object> packagesJson = new LinkedHashMap<>();
    packagesJson.put(PACKAGE_JSON_KEYS.get("PROVIDERS_URL_KEY"), repository.getUrl() + PACKAGE_JSON_PATH);
    packagesJson.put(PACKAGE_JSON_KEYS.get("PROVIDERS_KEY"), names.stream()
        .collect(Collectors.toMap((each) -> each, (each) -> singletonMap(PACKAGE_JSON_KEYS.get("SHA256_KEY"), null))));
    return new Content(new StringPayload(mapper.writeValueAsString(packagesJson), ContentTypes.APPLICATION_JSON));
  }

  /**
   * Rewrites the provider JSON so that source entries are removed and dist entries are pointed back to Nexus.
   */
  public Payload rewriteProviderJson(final Repository repository, final Payload payload) throws IOException {
    Map<String, Object> json = parseJson(payload);
    if (json.get(PACKAGE_JSON_KEYS.get("PACKAGES_KEY")) instanceof Map) {
      Map<String, Object> packagesMap = (Map<String, Object>) json.get(PACKAGE_JSON_KEYS.get("PACKAGES_KEY"));
      for (String packageName : packagesMap.keySet()) {
        Map<String, Object> packageVersions = (Map<String, Object>) packagesMap.get(packageName);
        for (String packageVersion : packageVersions.keySet()) {
          // TODO: Make this more robust, right now it makes a lot of assumptions and doesn't deal with bad things well
          Map<String, Object> versionInfo = (Map<String, Object>) packageVersions.get(packageVersion);
          versionInfo.remove(PACKAGE_JSON_KEYS.get("SOURCE_KEY")); // TODO: For now don't allow sources, probably should make this configurable?

          Map<String, Object> distInfo = (Map<String, Object>) versionInfo.get(PACKAGE_JSON_KEYS.get("DIST_KEY"));
          if (distInfo != null && ZIP_TYPE.equals(distInfo.get(PACKAGE_JSON_KEYS.get("TYPE_KEY")))) {
            versionInfo.put(PACKAGE_JSON_KEYS.get("DIST_KEY"),
                buildDistInfo(repository, packageName, packageVersion, (String) distInfo.get(PACKAGE_JSON_KEYS.get("REFERENCE_KEY")),
                    (String) distInfo.get(PACKAGE_JSON_KEYS.get("SHASUM_KEY")), ZIP_TYPE));
          }
        }
      }
    }
    return new StringPayload(mapper.writeValueAsString(json), payload.getContentType());
  }

  /**
   * Builds a provider JSON file for a list of components. This minimal subset will contain the packages entries with
   * the name, version, and dist information for each component. A timestamp derived from the component's last updated
   * field and a uid derived from the component group/name/version and last updated time is also included in the JSON.
   */
  public Content buildProviderJson(final Repository repository,
                                   final StorageTx storageTx,
                                   final Iterable<Component> components) throws IOException
  {
    Map<String, Map<String, Object>> packages = new LinkedHashMap<>();
    for (Component component : components) {
      Asset asset = storageTx.firstAsset(component);
      BlobRef blobRef = asset.requireBlobRef();
      Blob blob = storageTx.requireBlob(blobRef);
      Map<String, Object> composerJson = composerJsonExtractor.extractFromZip(blob);

      String vendor = component.group();
      String project = component.name();
      String version = component.version();

      String name = vendor + "/" + project;
      String time = component.requireLastUpdated().withZone(DateTimeZone.UTC).toString(timeFormatter);

      if (!packages.containsKey(name)) {
        packages.put(name, new LinkedHashMap<>());
      }

      String sha1 = asset.getChecksum(HashAlgorithm.SHA1).toString();
      Map<String, Object> packagesForName = packages.get(name);
      packagesForName
          .put(version, buildPackageInfo(repository, name, version, sha1, sha1, ZIP_TYPE, time, composerJson));
    }

    return new Content(new StringPayload(mapper.writeValueAsString(singletonMap(PACKAGE_JSON_KEYS.get("PACKAGES_KEY"), packages)),
        ContentTypes.APPLICATION_JSON));
  }

  /**
   * Merges an incoming set of packages.json files.
   */
  public Content mergePackagesJson(final Repository repository, final List<Payload> payloads) throws IOException {
    Set<String> names = new HashSet<>();
    for (Payload payload : payloads) {
      Map<String, Object> json = parseJson(payload);
      Map<String, Object> providers = (Map<String, Object>) json.get(PACKAGE_JSON_KEYS.get("PROVIDERS_KEY"));
      names.addAll(providers.keySet());
    }
    return buildPackagesJson(repository, names);
  }

  /**
   * Merges incoming provider JSON files, producing a merged file containing only the minimal subset of fields that we
   * need to download artifacts.
   */
  public Content mergeProviderJson(final Repository repository, final List<Payload> payloads, final DateTime now)
      throws IOException
  {
    String currentTime = now.withZone(DateTimeZone.UTC).toString(timeFormatter);

    // TODO: Make this more robust, right now it makes a lot of assumptions and doesn't deal with bad things well,
    // can probably consolidate this with the handling for rewrites for proxy (or at least make it more rational).
    Map<String, Map<String, Object>> packages = new LinkedHashMap<>();
    for (Payload payload : payloads) {
      Map<String, Object> json = parseJson(payload);
      if (json.get(PACKAGE_JSON_KEYS.get("PACKAGES_KEY")) instanceof Map) {

        Map<String, Object> packagesMap = (Map<String, Object>) json.get(PACKAGE_JSON_KEYS.get("PACKAGES_KEY"));
        for (String packageName : packagesMap.keySet()) {

          Map<String, Object> packageVersions = (Map<String, Object>) packagesMap.get(packageName);
          for (String packageVersion : packageVersions.keySet()) {

            Map<String, Object> versionInfo = (Map<String, Object>) packageVersions.get(packageVersion);
            Map<String, Object> distInfo = (Map<String, Object>) versionInfo.get(PACKAGE_JSON_KEYS.get("DIST_KEY"));
            if (distInfo == null) {
              continue;
            }

            if (!packages.containsKey(packageName)) {
              packages.put(packageName, new LinkedHashMap<>());
            }

            String time = (String) versionInfo.get(TIME_KEY);
            if (time == null) {
              time = currentTime;
            }

            Map<String, Object> packagesForName = packages.get(packageName);
            packagesForName.putIfAbsent(packageVersion, buildPackageInfo(repository, packageName, packageVersion,
                (String) distInfo.get(PACKAGE_JSON_KEYS.get("REFERENCE_KEY")), (String) distInfo.get(PACKAGE_JSON_KEYS.get("SHASUM_KEY")),
                (String) distInfo.get(PACKAGE_JSON_KEYS.get("TYPE_KEY")), time, versionInfo));
          }
        }
      }
    }

    return new Content(new StringPayload(mapper.writeValueAsString(singletonMap(PACKAGE_JSON_KEYS.get("PACKAGES_KEY"), packages)),
        ContentTypes.APPLICATION_JSON));
  }

  private Map<String, Object> buildPackageInfo(final Repository repository,
                                               final String packageName,
                                               final String packageVersion,
                                               final String reference,
                                               final String shasum,
                                               final String type,
                                               final String time,
                                               final Map<String, Object> versionInfo)
  {
    Map<String, Object> newPackageInfo = new LinkedHashMap<>();
    newPackageInfo.put(PACKAGE_JSON_KEYS.get("NAME_KEY"), packageName);
    newPackageInfo.put(PACKAGE_JSON_KEYS.get("VERSION_KEY"), packageVersion);
    newPackageInfo.put(PACKAGE_JSON_KEYS.get("DIST_KEY"), buildDistInfo(repository, packageName, packageVersion, reference, shasum, type));
    newPackageInfo.put(PACKAGE_JSON_KEYS.get("TIME_KEY"), time);
    newPackageInfo.put(PACKAGE_JSON_KEYS.get("UID_KEY"), Integer.toUnsignedLong(
        Hashing.md5().newHasher()
            .putString(packageName, StandardCharsets.UTF_8)
            .putString(packageVersion, StandardCharsets.UTF_8)
            .putString(time, StandardCharsets.UTF_8)
            .hash()
            .asInt()));

    for (String KEY : PACKAGE_JSON_KEYS.values()) {
      if(versionInfo.containsKey(KEY)) {
        newPackageInfo.put(KEY, versionInfo.get(KEY));
      }
    }

    return newPackageInfo;
  }

  private Map<String, Object> buildDistInfo(final Repository repository,
                                            final String packageName,
                                            final String packageVersion,
                                            final String reference,
                                            final String shasum,
                                            final String type)
  {
    String packageNameParts[] = packageName.split("/");
    String packageVendor = packageNameParts[0];
    String packageProject = packageNameParts[1];
    Map<String, Object> newDistInfo = new LinkedHashMap<>();
    newDistInfo
        .put(PACKAGE_JSON_KEYS.get("URL_KEY"), repository.getUrl() + "/" + buildZipballPath(packageVendor, packageProject, packageVersion));
    newDistInfo.put(PACKAGE_JSON_KEYS.get("TYPE_KEY"), type);
    newDistInfo.put(PACKAGE_JSON_KEYS.get("REFERENCE_KEY"), reference);
    newDistInfo.put(PACKAGE_JSON_KEYS.get("SHASUM_KEY"), shasum);
    return newDistInfo;
  }

  /**
   * Obtains the dist URL for a particular vendor/project and version within a provider JSON payload.
   */
  public String getDistUrl(final String vendor, final String project, final String version, final Payload payload)
      throws IOException
  {
    String vendorAndProject = String.format(VENDOR_AND_PROJECT, vendor, project);
    Map<String, Object> json = parseJson(payload);
    Map<String, Object> packagesMap = (Map<String, Object>) json.get(PACKAGE_JSON_KEYS.get("PACKAGES_KEY"));
    Map<String, Object> packageInfo = (Map<String, Object>) packagesMap.get(vendorAndProject);
    Map<String, Object> versionInfo = (Map<String, Object>) packageInfo.get(version);
    Map<String, Object> distInfo = (Map<String, Object>) versionInfo.get(PACKAGE_JSON_KEYS.get("DIST_KEY"));
    return (String) distInfo.get(PACKAGE_JSON_KEYS.get("URL_KEY"));
  }

  private Map<String, Object> parseJson(final Payload payload) throws IOException {
    try (InputStream in = payload.openInputStream()) {
      TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() { };
      return mapper.readValue(in, typeReference);
    }
  }
}
