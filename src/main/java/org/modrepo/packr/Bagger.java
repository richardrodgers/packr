/**
 * Copyright 2023, Richard Rodgers
 * SPDX-Licence-Identifier: Apache-2.0
 */

package org.modrepo.packr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.modrepo.packr.Serde.*;

/**
 * Bagger is a command-line interface to the BagIt library.
 * It provides simple creation, serialization, and validation of Bags.
 *
 * See README for sample invocations.
 *
 * @author richardrodgers
 */

public class Bagger {
    /* A bit clunky in the cmd-line arg handling, but deliberately so as to limit
       external dependencies for those who want to only use the library API directly. */
    private final List<String> payloads = new ArrayList<>();
    private final List<String> references = new ArrayList<>();
    private final List<String> tags = new ArrayList<>();
    private final List<String> statements = new ArrayList<>();
    private final List<String> csAlgorithms = new ArrayList<>();
    private String basis;
    private String archFmt = "directory";
    private boolean noTime = false;
    private String defaultCsAlg = "SHA-512";
    private Charset tagEnc = Charset.defaultCharset();
    private final List<String> optFlags = new ArrayList<>();
    private int verbosityLevel;

    public static void main(String[] args) throws IOException, IllegalAccessException, URISyntaxException {
        if (args.length < 2) {
            usage();
        }
        Bagger bagger = new Bagger();
        int i = 2;
        while (i < args.length) {
            switch(args[i]) {
                case "-p": bagger.payloads.add(args[i+1]); break;
                case "-r": bagger.references.add(args[i+1]); break;
                case "-t": bagger.tags.add(args[i+1]); break;
                case "-m": bagger.statements.add(args[i+1]); break;
                case "-b": bagger.basis = args[i+1]; break;
                case "-n": bagger.noTime = Boolean.valueOf(args[i+1]); break;
                case "-a": bagger.archFmt = args[i+1]; break;
                case "-c": bagger.csAlgorithms.add(args[i+1]); break;
                case "-e": bagger.tagEnc = Charset.forName(args[i+1]); break;
                case "-o": bagger.optFlags.add(args[i+1]); break;
                case "-v": bagger.verbosityLevel = Integer.parseInt(args[i+1]); break;
                default: System.out.println("Unknown option: '" + args[i] + "'"); usage();
            }
            i += 2;
        }
        // execute command if recognized
        Path bagName = Paths.get(args[1]).toAbsolutePath();
        switch(args[0]) {
            case "fill" : bagger.fill(bagName); break;
            case "complete" : bagger.complete(bagName); break;
            case "validate" : bagger.validate(bagName); break;
            default: System.out.println("Unknown command: '" + args[0] + "'"); usage();
        }
    }
 
    /**
     * Emit usage string for Bagger to console
     */
    public static void usage() {
        System.out.println(
            "Usage: Bagger command bagName [-options]\n" +
            "Commands:\n" +
            "fill       fill a bag with contents\n" +
            "complete   returns 0 if bag complete, else non-zero\n" +
            "validate   returns 0 if bag valid, else non-zero");
        System.out.println(
            "Options:\n" +
            "-p    [<bag path>=]<payload file>\n" +
            "-r    [<bag path>=]<payload file>=<uri>\n" +
            "-t    [<bag path>=]<tag file>\n" +
            "-m    <name>=<value> - metadata statement\n" +
            "-b    <basis bag path> - basis bag for fill\n" +
            "-a    <archive format> - e.g. 'zip', 'tar', 'tgz', (default: loose directory)\n" +
            "-n    <noTime> - 'true' or 'false'\n" +
            "-c    <checksum algorithm> - default: 'SHA-512'\n" +
            "-e    <tag file encoding> - default: 'UTF-8'\n" +
            "-o    <optimization flag>\n" +
            "-v    <level> - output level to console (default: 0 = no output)");
        System.out.println(
            "Optimization flags:\n" +
            "nag   suppress automatic metadata generation");
        System.exit(1);
    }

    private Bagger() {}

    private void fill(Path baseDir) throws IOException, IllegalAccessException, URISyntaxException {

        BagBuilder builder = null;
        if (basis != null) {
            if (csAlgorithms.isEmpty()) {
                // use existing bag's checksum algorithms
                builder = Adapter.copy(baseDir, Serde.fromDirectory(Paths.get(basis), false));
            } else {
                builder = Adapter.copy(baseDir, Serde.fromDirectory(Paths.get(basis), false), csAlgorithms.toArray(String[]::new));
            }
        } else {
            if (csAlgorithms.isEmpty()) {
                // use default
                csAlgorithms.add(defaultCsAlg);
            }
            builder = new BagBuilder(baseDir, tagEnc, csAlgorithms.toArray(String[]::new));
        }
        if (optFlags.contains("nag")) {
            builder.autoGenerate(new HashSet<>());
        }

        for (String payload : payloads) {
            if (payload.indexOf("=") > 0) {
                String[] parts = payload.split("=");
                builder.payload(parts[0], Paths.get(parts[1]));
            } else {
                builder.payload(payload, Paths.get(payload));
            }
        }
        for (String reference : references) {
            String[] parts = reference.split("=");
            URI uri = new URI(parts[2]);
            builder.payloadRef(parts[0], Paths.get(parts[1]), uri);
        }
        for (String tag : tags) {
            if (tag.indexOf("=") > 0) {
                String[] parts = tag.split("=");
                builder.tag(parts[0], Paths.get(parts[1]));
            } else {
                builder.tag(tag, Paths.get(tag));
            }
        }
        for (String statement : statements) {
            String[] parts = statement.split("=");
            builder.metadata(parts[0], parts[1]);
        }
        Bag bag = builder.build();
        Path bagPath;
        if (archFmt.equals("directory")) {
            bagPath = baseDir;
        } else {
            bagPath = toPackage(bag, archFmt, noTime, Optional.of(baseDir));
        }
        if (verbosityLevel > 0) {
            message(bagPath.getFileName().toString(), true, "created");
        }
    }

    private void complete(Path bagPath) throws IOException {
        int status = Serde.fromDirectory(bagPath, false).completeStatus();
        boolean complete = status == 0;
        if (verbosityLevel > 0) {
           message(bagPath.getFileName().toString(), complete, "complete");
        }
        System.exit(status);
    }

    private void validate(Path bagPath) throws IOException {
        int status = Serde.fromDirectory(bagPath, false).validationStatus();
        boolean valid = status == 0;
        if (verbosityLevel > 0) {
            message(bagPath.getFileName().toString(), valid, "valid");
        }
        System.exit(status);
    }

    private void message(String name, boolean ok, String value) {
        StringBuilder sb = new StringBuilder("Bag '");
        sb.append(name).append("' is ");
        if (! ok) sb.append("in");
        sb.append(value);
        System.out.println(sb.toString());
    }
}
