// We use ES modules for cleaner dependency management
import init, {
    calculate_bmt_hash,
    benchmark_hash,
    get_library_info,
    generate_svg_icon,
    create_icon_from_hex,
    generate_random_chunk_address,
    benchmark_hash_with_random_data,
    IconConfig,
    IconShape,
    GeneratorFunction,
    ColorScheme,
    IconData,
    create_content_chunk,
    generate_random_private_key,
    get_address_from_private_key,
    create_single_owner_chunk,
    generate_random_chunk_id,
    analyze_chunk,
    generate_svg_for_address,
    ChunkType,
} from "./bmt-wasm-demo.js";

// Initialize the WASM module
async function initWasm() {
    try {
        // Initialize the WASM module
        await init();
        return true;
    } catch (error) {
        console.error("Error initializing WASM module:", error);
        document.body.innerHTML = `
            <div class="error">
                <h2>Failed to load WASM Module</h2>
                <p>Error: ${error.message}</p>
                <p>Make sure you've built the WASM package correctly.</p>
            </div>
        `;
        return false;
    }
}

// Main application initialization
async function initApp() {
    // First load the WASM module
    const wasmLoaded = await initWasm();
    if (!wasmLoaded) return;

    // Display library info
    document.getElementById("library-info").textContent = get_library_info();

    // Setup tabs
    setupTabs();

    // Setup BMT hasher tab
    setupBmtHasher();

    // Setup icon generator tab
    setupIconGenerator();

    // Setup benchmarks tab
    setupBenchmarks();

    // Setup chunk creator tab
    setupChunkCreator();
}

// Set up tab switching
function setupTabs() {
    const tabButtons = document.querySelectorAll(".tab-button");
    const tabPanes = document.querySelectorAll(".tab-pane");

    tabButtons.forEach((button) => {
        button.addEventListener("click", () => {
            // Remove active class from all buttons and panes
            tabButtons.forEach((btn) => btn.classList.remove("active"));
            tabPanes.forEach((pane) => pane.classList.remove("active"));

            // Add active class to clicked button and corresponding pane
            button.classList.add("active");
            const tabId = button.getAttribute("data-tab");
            document.getElementById(tabId).classList.add("active");
        });
    });
}

// Set up the BMT hasher tab functionality
function setupBmtHasher() {
    const textInput = document.getElementById("text-input");
    const hashResult = document.getElementById("hash-result");
    const textLength = document.getElementById("text-length");
    const spanInput = document.getElementById("span-input");
    const byteViz = document.getElementById("byte-viz");
    const copyButton = document.getElementById("copy-button");
    const downloadBmtIcon = document.getElementById("download-bmt-icon");

    // Icon config elements
    const hasherIconGenerator = document.getElementById(
        "hasher-icon-generator",
    );
    const hasherIconShape = document.getElementById("hasher-icon-shape");
    const iconConfig = document.querySelector(".icon-config");

    // Function to update the hash and visualization
    function updateHash() {
        const text = textInput.value;
        const textLen = text.length;
        const span = parseInt(spanInput.value, 10) || 0;

        textLength.textContent = textLen;

        // Calculate the hash
        const result = calculate_bmt_hash(text, span);
        hashResult.textContent = result.hex;
        updateByteVisualization(result.bytes);

        // If icon config is open, also update the icon visualization
        if (iconConfig.open) {
            updateIconVisualization(result.bytes);
        }
    }

    // Create the byte visualization grid
    function updateByteVisualization(bytes) {
        byteViz.innerHTML = "";

        // Loop through each byte
        for (let i = 0; i < bytes.length; i++) {
            const byte = bytes[i];
            const byteEl = document.createElement("div");
            byteEl.classList.add("byte");

            // Get the byte value and create a color based on it
            const hue = Math.floor((byte / 255) * 360);
            byteEl.style.backgroundColor = `hsl(${hue}, 80%, 60%)`;
            byteEl.setAttribute(
                "title",
                `Byte ${i}: ${byte} (0x${byte.toString(16).padStart(2, "0")})`,
            );

            byteViz.appendChild(byteEl);
        }
    }

    // Update icon visualization from hash
    function updateIconVisualization(bytes) {
        try {
            // Create the icon config based on the settings
            const config = new IconConfig(
                200,
                hasherIconShape.value === "Circle"
                    ? IconShape.Circle
                    : IconShape.Square,
                GeneratorFunction[hasherIconGenerator.value],
                getSelectedColorScheme("hasher-color-scheme"),
            );

            // Create a chunk from the hash bytes
            const iconData = create_icon_from_hex(
                Array.from(bytes)
                    .map((b) => b.toString(16).padStart(2, "0"))
                    .join(""),
                "01", // Default type
                "01", // Default version
                "", // Empty header
                "", // Empty payload
            );

            // Generate the SVG
            const svgContent = generate_svg_icon(iconData, config);
            document.getElementById("bmt-icon-preview").innerHTML = svgContent;
        } catch (error) {
            console.error("Error generating icon from hash:", error);
            document.getElementById("bmt-icon-preview").innerHTML =
                `<div class="error-message">Error generating icon: ${error.message}</div>`;
        }
    }

    // Helper function to get the selected color scheme
    function getSelectedColorScheme(radioName) {
        const selected = document.querySelector(
            `input[name="${radioName}"]:checked`,
        ).value;
        return ColorScheme[selected];
    }

    // Set up event listeners
    textInput.addEventListener("input", () => {
        // Update span input to match text length when text changes
        spanInput.value = textInput.value.length;
        updateHash();
    });

    spanInput.addEventListener("input", updateHash);

    // Update when icon config changes
    hasherIconGenerator.addEventListener("change", () => {
        if (iconConfig.open) updateHash();
    });

    hasherIconShape.addEventListener("change", () => {
        if (iconConfig.open) updateHash();
    });

    document
        .querySelectorAll('input[name="hasher-color-scheme"]')
        .forEach((radio) => {
            radio.addEventListener("change", () => {
                if (iconConfig.open) updateHash();
            });
        });

    // When the icon config is opened, update the icon
    iconConfig.addEventListener("toggle", () => {
        if (iconConfig.open) {
            const bytes = new Uint8Array(
                hashResult.textContent
                    .slice(2)
                    .match(/.{1,2}/g)
                    .map((byte) => parseInt(byte, 16)),
            );
            updateIconVisualization(bytes);
        }
    });

    // Copy button functionality
    copyButton.addEventListener("click", () => {
        navigator.clipboard
            .writeText(hashResult.textContent)
            .then(() => {
                copyButton.textContent = "Copied!";
                setTimeout(() => {
                    copyButton.textContent = "Copy";
                }, 2000);
            })
            .catch((err) => {
                console.error("Failed to copy: ", err);
            });
    });

    // Download icon button
    downloadBmtIcon.addEventListener("click", () => {
        const svgContent =
            document.getElementById("bmt-icon-preview").innerHTML;
        if (svgContent) {
            downloadSvg(svgContent, "bmt-hash-icon.svg");
        }
    });

    // Initialize by calculating BMT hash of empty array
    updateHash();
}

// Helper function to download SVG
function downloadSvg(svgContent, filename) {
    const blob = new Blob([svgContent], { type: "image/svg+xml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Convert bytes to hex string
function bytesToHex(bytes) {
    return Array.from(bytes)
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
}

// Set up the icon generator tab
function setupIconGenerator() {
    const generateButton = document.getElementById("generate-icon");
    const randomAddressButton = document.getElementById(
        "generate-random-address",
    );
    const copyButton = document.getElementById("copy-svg");
    const downloadButton = document.getElementById("download-svg");
    const chunkAddress = document.getElementById("chunk-address");
    const chunkType = document.getElementById("chunk-type");
    const chunkVersion = document.getElementById("chunk-version");
    const headerData = document.getElementById("header-data");
    const payloadData = document.getElementById("payload-data");
    const iconGenerator = document.getElementById("icon-generator");
    const iconShape = document.getElementById("icon-shape");
    const iconPreview = document.getElementById("icon-preview");
    const svgOutput = document.getElementById("svg-output");

    // Generate icon when button is clicked
    generateButton.addEventListener("click", generateIconFromInputs);

    // Generate a random address
    randomAddressButton.addEventListener("click", () => {
        const randomBytes = generate_random_chunk_address();
        chunkAddress.value = bytesToHex(randomBytes);
    });

    // Copy SVG to clipboard
    copyButton.addEventListener("click", () => {
        navigator.clipboard
            .writeText(svgOutput.textContent)
            .then(() => {
                copyButton.textContent = "Copied!";
                setTimeout(() => (copyButton.textContent = "Copy SVG"), 2000);
            })
            .catch((err) => console.error("Failed to copy: ", err));
    });

    // Download SVG
    downloadButton.addEventListener("click", () => {
        const svgContent = svgOutput.textContent;
        if (svgContent) {
            downloadSvg(svgContent, "chunk-icon.svg");
        }
    });

    // Function to generate icon from input values
    function generateIconFromInputs() {
        try {
            // Create icon data from inputs
            const iconData = create_icon_from_hex(
                chunkAddress.value,
                chunkType.value,
                chunkVersion.value,
                headerData.value,
                payloadData.value,
            );

            // Create icon config
            const config = new IconConfig(
                200,
                iconShape.value === "Circle"
                    ? IconShape.Circle
                    : IconShape.Square,
                GeneratorFunction[iconGenerator.value],
                getSelectedColorScheme("color-scheme"),
            );

            // Generate the SVG
            const svgContent = generate_svg_icon(iconData, config);

            // Display the SVG
            iconPreview.innerHTML = svgContent;
            svgOutput.textContent = svgContent;
        } catch (error) {
            console.error("Error generating icon:", error);
            alert(`Error: ${error}`);
        }
    }

    // Helper function to get the selected color scheme
    function getSelectedColorScheme(radioName) {
        const selected = document.querySelector(
            `input[name="${radioName}"]:checked`,
        ).value;
        return ColorScheme[selected];
    }

    // Generate example icons
    generateExampleIcons();

    // Function to generate example icons
    function generateExampleIcons() {
        const container = document.getElementById("example-icons");
        container.innerHTML = "";

        // Generate 5 examples with different configurations
        for (let i = 0; i < 5; i++) {
            try {
                // Generate random bytes for the address
                const randomBytes = generate_random_chunk_address();
                const chunkType = Math.floor(Math.random() * 256);
                const version = Math.floor(Math.random() * 256);

                // Create a chunk with the random data
                const iconData = create_icon_from_hex(
                    bytesToHex(randomBytes),
                    chunkType.toString(16).padStart(2, "0"),
                    version.toString(16).padStart(2, "0"),
                    "",
                    "",
                );

                // Select a generator function and shape
                const generators = [
                    GeneratorFunction.Geometric,
                    GeneratorFunction.Abstract,
                    GeneratorFunction.Circular,
                    GeneratorFunction.Pixelated,
                    GeneratorFunction.Molecular,
                ];

                const shapes = [IconShape.Square, IconShape.Circle];
                const colorSchemes = [
                    ColorScheme.Vibrant,
                    ColorScheme.Pastel,
                    ColorScheme.Monochrome,
                    ColorScheme.Complementary,
                ];

                // Create a config for this example
                const config = new IconConfig(
                    80, // smaller size for examples
                    shapes[i % 2],
                    generators[i % 5],
                    colorSchemes[i % 4],
                );

                // Generate the SVG
                const svgContent = generate_svg_icon(iconData, config);

                // Create the example element
                const example = document.createElement("div");
                example.className = "example-icon";
                example.innerHTML = svgContent;
                example.title = `Example ${i + 1}`;

                // Make the example clickable
                example.addEventListener("click", () => {
                    // Apply this example's settings
                    iconGenerator.value = Object.keys(GeneratorFunction)[i % 5];
                    iconShape.value = i % 2 === 0 ? "Square" : "Circle";

                    // Set the color scheme radio button
                    const colorSchemeValue = Object.keys(ColorScheme)[i % 4];
                    document.querySelector(
                        `input[name="color-scheme"][value="${colorSchemeValue}"]`,
                    ).checked = true;

                    // Generate with current inputs but new config
                    generateIconFromInputs();
                });

                container.appendChild(example);
            } catch (error) {
                console.error("Error generating example:", error);
            }
        }
    }

    // Set initial values and generate the first icon
    if (!chunkAddress.value) {
        const randomBytes = generate_random_chunk_address();
        chunkAddress.value = bytesToHex(randomBytes);
    }

    generateIconFromInputs();
}

// Format a number with thousands separators
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Set up the benchmarks tab
function setupBenchmarks() {
    const benchmarkSizeSelect = document.getElementById("benchmark-size");
    const benchmarkIterationsInput = document.getElementById(
        "benchmark-iterations",
    );
    const runBenchmarkButton = document.getElementById("run-benchmark");
    const benchmarkOutput = document.getElementById("benchmark-output");
    const resultPlaceholder = document.getElementById("result-placeholder");

    runBenchmarkButton.addEventListener("click", async () => {
        const size = parseInt(benchmarkSizeSelect.value, 10);
        const iterations = parseInt(benchmarkIterationsInput.value, 10);

        if (isNaN(size) || isNaN(iterations) || iterations < 1) {
            alert("Please enter valid values for size and iterations");
            return;
        }

        // Ensure size doesn't exceed max test size
        const testSize = Math.min(size, 4096);

        // Calculate total data size needed
        const totalDataSize = testSize * iterations;

        // Check if the data size is reasonable
        const totalSizeMB = totalDataSize / (1024 * 1024);
        if (totalSizeMB > 500) {
            const confirmed = confirm(
                `This benchmark will generate ${totalSizeMB.toFixed(2)} MB of random data. ` +
                    `This might cause high memory usage. Continue?`,
            );
            if (!confirmed) return;
        }

        // Update UI to show benchmark is running
        runBenchmarkButton.disabled = true;
        runBenchmarkButton.textContent = "Running...";
        resultPlaceholder.textContent = `Generating ${formatDataSize(totalDataSize)} of random data...`;
        benchmarkOutput.innerHTML = "";

        // Add a small delay to allow UI to update
        await new Promise((resolve) => setTimeout(resolve, 50));

        try {
            // Generate a buffer with unique random data for each iteration
            const startDataGen = performance.now();

            // Create the buffer for all iterations
            const randomData = new Uint8Array(totalDataSize);

            // Fill the buffer with random data
            // For large buffers, we'll fill in chunks to avoid UI freezing
            const CHUNK_SIZE = 1024 * 1024; // 1MB chunks for generation

            for (let offset = 0; offset < totalDataSize; offset += CHUNK_SIZE) {
                // If we've been generating data for more than 500ms, update the UI
                const currentTime = performance.now();
                if (currentTime - startDataGen > 500) {
                    const percentComplete = Math.min(
                        100,
                        (offset / totalDataSize) * 100,
                    ).toFixed(1);
                    resultPlaceholder.textContent = `Generating random data: ${percentComplete}% complete...`;
                    // Allow UI to update
                    await new Promise((resolve) => setTimeout(resolve, 0));
                }

                // Calculate the size of this chunk
                const end = Math.min(offset + CHUNK_SIZE, totalDataSize);
                const currentChunkSize = end - offset;

                // Generate random data for this chunk
                for (let i = 0; i < currentChunkSize; i++) {
                    randomData[offset + i] = Math.floor(Math.random() * 256);
                }
            }

            const dataGenTime = (performance.now() - startDataGen) / 1000; // in seconds

            // Update status
            resultPlaceholder.textContent = "Running benchmark...";
            await new Promise((resolve) => setTimeout(resolve, 50));

            // Run the benchmark with completely random data for each iteration
            const avgTime = benchmark_hash_with_random_data(
                randomData,
                testSize,
                iterations,
            );

            // Check for error
            if (avgTime < 0) {
                throw new Error(
                    "Benchmark failed - insufficient data provided",
                );
            }

            // avgTime is in milliseconds per hash operation
            const millisPerOp = avgTime;

            // Convert to operations per second
            const opsPerSecond = 1000 / millisPerOp;

            // Calculate throughput in bytes per second
            const throughput = testSize * opsPerSecond;

            // Display results
            resultPlaceholder.textContent = "";
            benchmarkOutput.innerHTML = `
                <div class="benchmark-result-item">
                    <strong>Data Size:</strong> ${formatNumber(testSize)} bytes (${(testSize / 1024).toFixed(2)} KB)
                </div>
                <div class="benchmark-result-item">
                    <strong>Iterations:</strong> ${formatNumber(iterations)}
                </div>
                <div class="benchmark-result-item">
                    <strong>Total Random Data:</strong> ${formatDataSize(totalDataSize)}
                    <br><small>(generated in ${dataGenTime.toFixed(2)}s)</small>
                </div>
                <div class="benchmark-result-item">
                    <strong>Average Time:</strong> ${millisPerOp.toFixed(3)} ms per hash
                </div>
                <div class="benchmark-result-item">
                    <strong>Operations:</strong> ${opsPerSecond.toFixed(2)} hashes/second
                </div>
                <div class="benchmark-result-item">
                    <strong>Throughput:</strong> ${formatDataSize(throughput)}/second
                </div>
            `;
        } catch (err) {
            resultPlaceholder.textContent = "Error during benchmark";
            console.error("Benchmark error:", err);
        } finally {
            // Reset button state
            runBenchmarkButton.disabled = false;
            runBenchmarkButton.textContent = "Run Benchmark";
        }
    });

    // Helper function to format data sizes
    function formatDataSize(bytes) {
        if (bytes < 1024) {
            return `${bytes.toFixed(2)} B`;
        } else if (bytes < 1024 * 1024) {
            return `${(bytes / 1024).toFixed(2)} KB`;
        } else if (bytes < 1024 * 1024 * 1024) {
            return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
        } else {
            return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
        }
    }
}

// Setup the chunk creator tab
function setupChunkCreator() {
    // Mode switching
    const createModeBtn = document.getElementById("create-mode-btn");
    const analyzeModeBtn = document.getElementById("analyze-mode-btn");
    const createMode = document.getElementById("chunk-create-mode");
    const analyzeMode = document.getElementById("chunk-analyze-mode");

    createModeBtn.addEventListener("click", () => {
        createModeBtn.classList.add("active");
        analyzeModeBtn.classList.remove("active");
        createMode.style.display = "block";
        analyzeMode.style.display = "none";
    });

    analyzeModeBtn.addEventListener("click", () => {
        analyzeModeBtn.classList.add("active");
        createModeBtn.classList.remove("active");
        analyzeMode.style.display = "block";
        createMode.style.display = "none";
    });

    // Chunk type switching
    const contentChunkType = document.getElementById("content-chunk-type");
    const singleOwnerChunkType = document.getElementById(
        "single-owner-chunk-type",
    );
    const contentChunkForm = document.getElementById("content-chunk-form");
    const singleOwnerChunkForm = document.getElementById(
        "single-owner-chunk-form",
    );

    contentChunkType.addEventListener("change", () => {
        if (contentChunkType.checked) {
            contentChunkForm.style.display = "block";
            singleOwnerChunkForm.style.display = "none";
        }
    });

    singleOwnerChunkType.addEventListener("change", () => {
        if (singleOwnerChunkType.checked) {
            singleOwnerChunkForm.style.display = "block";
            contentChunkForm.style.display = "none";
        }
    });

    // Input type toggle for content chunk
    setupInputToggle("content-chunk-data");
    setupInputToggle("so-chunk-data");

    // Size counters
    const contentChunkData = document.getElementById("content-chunk-data");
    const contentDataSize = document.getElementById("content-data-size");
    const contentDataWarning = document.getElementById("content-data-warning");

    contentChunkData.addEventListener("input", () => {
        updateDataSize(contentChunkData, contentDataSize, contentDataWarning);
    });

    const soChunkData = document.getElementById("so-chunk-data");
    const soDataSize = document.getElementById("so-data-size");
    const soDataWarning = document.getElementById("so-data-warning");

    soChunkData.addEventListener("input", () => {
        updateDataSize(soChunkData, soDataSize, soDataWarning);
    });

    // Random generators
    const generateChunkId = document.getElementById("generate-chunk-id");
    const soChunkId = document.getElementById("so-chunk-id");

    generateChunkId.addEventListener("click", () => {
        const randomId = generate_random_chunk_id();
        soChunkId.value = "0x" + bytesToHex(randomId);
    });

    const generatePrivateKey = document.getElementById("generate-private-key");
    const privateKey = document.getElementById("private-key");
    const ownerAddressContainer = document.getElementById(
        "owner-address-container",
    );
    const ownerAddress = document.getElementById("owner-address");

    generatePrivateKey.addEventListener("click", () => {
        const randomPk = generate_random_private_key();
        privateKey.value = "0x" + bytesToHex(randomPk);
        updateOwnerAddress();
    });

    privateKey.addEventListener("input", updateOwnerAddress);

    function updateOwnerAddress() {
        const pkValue = privateKey.value.trim();
        if (pkValue.length === 66 || pkValue.length === 64) {
            try {
                const pkBytes = hexToBytes(pkValue);
                const addressBytes = get_address_from_private_key(pkBytes);
                ownerAddress.textContent = "0x" + bytesToHex(addressBytes);
                ownerAddressContainer.style.display = "block";
            } catch (error) {
                ownerAddressContainer.style.display = "none";
            }
        } else {
            ownerAddressContainer.style.display = "none";
        }
    }

    // Create content chunk
    const createContentChunkBtn = document.getElementById(
        "create-content-chunk",
    );
    createContentChunkBtn.addEventListener("click", createContentChunk);

    // Create single owner chunk
    const createSoChunkBtn = document.getElementById("create-so-chunk");
    createSoChunkBtn.addEventListener("click", createSingleOwnerChunk);

    // Analyze chunk
    const analyzeChunkBtn = document.getElementById("analyze-chunk-btn");
    analyzeChunkBtn.addEventListener("click", analyzeChunk);

    // Result actions
    const copySerializedChunk = document.getElementById(
        "copy-serialized-chunk",
    );
    copySerializedChunk.addEventListener("click", () => {
        const serializedData = document.getElementById(
            "serialized-chunk-data",
        ).textContent;
        navigator.clipboard.writeText(serializedData).then(() => {
            copySerializedChunk.textContent = "Copied!";
            setTimeout(() => {
                copySerializedChunk.textContent = "Copy Serialized Chunk";
            }, 2000);
        });
    });

    const downloadSerializedChunk = document.getElementById(
        "download-serialized-chunk",
    );
    downloadSerializedChunk.addEventListener("click", () => {
        const serializedData = document.getElementById(
            "serialized-chunk-data",
        ).textContent;
        const chunkType =
            document.getElementById("result-chunk-type").textContent;
        const fileName = `${chunkType.toLowerCase().replace(/\s+/g, "-")}-${Date.now()}.hex`;

        const blob = new Blob([serializedData], { type: "text/plain" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    });

    const createNewChunk = document.getElementById("create-new-chunk");
    createNewChunk.addEventListener("click", () => {
        document.getElementById("chunk-creation-result").style.display = "none";
    });

    const copyAnalyzedData = document.getElementById("copy-analyzed-data");
    copyAnalyzedData.addEventListener("click", () => {
        // Get address from analysis
        const address = document.getElementById(
            "analysis-chunk-address",
        ).textContent;
        navigator.clipboard.writeText(address).then(() => {
            copyAnalyzedData.textContent = "Copied!";
            setTimeout(() => {
                copyAnalyzedData.textContent = "Copy Address";
            }, 2000);
        });
    });

    const analyzeNewChunk = document.getElementById("analyze-new-chunk");
    analyzeNewChunk.addEventListener("click", () => {
        document.getElementById("chunk-analysis-result").style.display = "none";
        document.getElementById("chunk-data-hex").value = "";
        document.getElementById("expected-address").value = "";
    });

    // Copy functionality for all copy-enabled elements
    document.querySelectorAll(".copy-enabled").forEach((el) => {
        el.addEventListener("click", function () {
            navigator.clipboard.writeText(this.textContent).then(() => {
                const originalText = this.textContent;
                this.textContent = "Copied!";
                setTimeout(() => {
                    this.textContent = originalText;
                }, 1000);
            });
        });
    });
}

// Helper functions for chunk creation/analysis
function setupInputToggle(inputId) {
    const textarea = document.getElementById(inputId);
    const toggleOptions =
        textarea.parentElement.querySelectorAll(".toggle-option");

    toggleOptions.forEach((option) => {
        option.addEventListener("click", () => {
            // Remove active class from all options
            toggleOptions.forEach((opt) => opt.classList.remove("active"));
            // Add active class to clicked option
            option.classList.add("active");

            // Convert data if needed
            const format = option.getAttribute("data-format");
            convertInputFormat(textarea, format);
        });
    });
}

function convertInputFormat(textarea, targetFormat) {
    const currentText = textarea.value.trim();
    if (!currentText) return;

    const currentFormat = textarea.parentElement
        .querySelector(".toggle-option.active")
        .getAttribute("data-format");

    // Only convert if changing formats
    if (currentFormat !== targetFormat) {
        try {
            if (currentFormat === "text" && targetFormat === "hex") {
                // Text to hex
                const encoder = new TextEncoder();
                const bytes = encoder.encode(currentText);
                textarea.value = "0x" + bytesToHex(bytes);
            } else if (currentFormat === "hex" && targetFormat === "text") {
                // Hex to text
                const bytes = hexToBytes(currentText);
                const decoder = new TextDecoder();
                textarea.value = decoder.decode(bytes);
            }
        } catch (error) {
            // If conversion fails, show error and revert to original format
            alert(
                `Cannot convert the current input to ${targetFormat} format: ${error.message}`,
            );
            textarea.parentElement
                .querySelector(`[data-format="${currentFormat}"]`)
                .classList.add("active");
            textarea.parentElement
                .querySelector(`[data-format="${targetFormat}"]`)
                .classList.remove("active");
        }
    }
}

function updateDataSize(textarea, sizeElement, warningElement) {
    const text = textarea.value.trim();
    let byteLength = 0;

    const format = textarea.parentElement
        .querySelector(".toggle-option.active")
        .getAttribute("data-format");

    if (format === "text") {
        const encoder = new TextEncoder();
        byteLength = encoder.encode(text).length;
    } else if (format === "hex") {
        try {
            const bytes = hexToBytes(text);
            byteLength = bytes.length;
        } catch (error) {
            warningElement.textContent = "Invalid hex format";
            return;
        }
    }

    sizeElement.textContent = byteLength;

    // Check if size exceeds max
    if (byteLength > 4096) {
        warningElement.textContent =
            "Warning: Exceeds maximum chunk size (4096 bytes)";
    } else {
        warningElement.textContent = "";
    }
}

function hexToBytes(hexString) {
    // Remove 0x prefix if present
    hexString = hexString.startsWith("0x") ? hexString.slice(2) : hexString;

    // Validate hex string
    if (hexString.length % 2 !== 0 || !/^[0-9a-fA-F]+$/.test(hexString)) {
        throw new Error("Invalid hex string");
    }

    const bytes = new Uint8Array(hexString.length / 2);
    for (let i = 0; i < hexString.length; i += 2) {
        bytes[i / 2] = parseInt(hexString.substr(i, 2), 16);
    }

    return bytes;
}

function getDataFromInput(inputElement) {
    const text = inputElement.value.trim();
    if (!text) {
        throw new Error("Input data cannot be empty");
    }

    const format = inputElement.parentElement
        .querySelector(".toggle-option.active")
        .getAttribute("data-format");

    if (format === "text") {
        const encoder = new TextEncoder();
        return encoder.encode(text);
    } else if (format === "hex") {
        return hexToBytes(text);
    }
}

function createContentChunk() {
    try {
        const data = getDataFromInput(
            document.getElementById("content-chunk-data"),
        );

        // Check size
        if (data.length > 4096) {
            alert("Data exceeds maximum chunk size (4096 bytes)");
            return;
        }

        // Create the chunk
        const result = create_content_chunk(data);

        // Show result
        displayContentChunkResult(result);
    } catch (error) {
        alert(`Error creating content chunk: ${error.message}`);
    }
}

function createSingleOwnerChunk() {
    try {
        // Get ID
        const idInput = document.getElementById("so-chunk-id").value.trim();
        if (!idInput) {
            alert("Chunk ID is required");
            return;
        }
        const id = hexToBytes(idInput);
        if (id.length !== 32) {
            alert("Chunk ID must be exactly 32 bytes (64 hex characters)");
            return;
        }

        // Get private key
        const pkInput = document.getElementById("private-key").value.trim();
        if (!pkInput) {
            alert("Private key is required");
            return;
        }
        const privateKey = hexToBytes(pkInput);
        if (privateKey.length !== 32) {
            alert("Private key must be exactly 32 bytes (64 hex characters)");
            return;
        }

        // Get data
        const data = getDataFromInput(document.getElementById("so-chunk-data"));

        // Check size
        if (data.length > 4096) {
            alert("Data exceeds maximum chunk size (4096 bytes)");
            return;
        }

        // Create the chunk
        const result = create_single_owner_chunk(id, data, privateKey);

        // Show result
        displaySingleOwnerChunkResult(result);
    } catch (error) {
        alert(`Error creating single owner chunk: ${error.message}`);
    }
}

function displayContentChunkResult(result) {
    // Set result type
    document.getElementById("result-chunk-type").textContent = "Content Chunk";

    // Set address
    document.getElementById("result-chunk-address").textContent =
        result.address_hex;

    // Hide ID and owner fields
    document.getElementById("result-id-field").style.display = "none";
    document.getElementById("result-owner-field").style.display = "none";

    // Set sizes
    document.getElementById("result-data-size").textContent =
        `${result.data.length} bytes`;
    document.getElementById("result-total-size").textContent =
        `${result.size} bytes`;

    // Set serialized data
    document.getElementById("serialized-chunk-data").textContent =
        result.serialized_hex;

    // Generate icon
    const iconConfig = new IconConfig(
        200,
        IconShape.Circle,
        GeneratorFunction.Geometric,
        ColorScheme.Vibrant,
    );

    const svgContent = generate_svg_for_address(result.address, iconConfig);
    document.getElementById("created-chunk-icon").innerHTML = svgContent;

    // Show the result container
    document.getElementById("chunk-creation-result").style.display = "block";
}

function displaySingleOwnerChunkResult(result) {
    // Set result type
    document.getElementById("result-chunk-type").textContent =
        "Single Owner Chunk";

    // Set address
    document.getElementById("result-chunk-address").textContent =
        result.address_hex;

    // Set ID and owner fields
    document.getElementById("result-id-field").style.display = "block";
    document.getElementById("result-chunk-id").textContent = result.id_hex;

    document.getElementById("result-owner-field").style.display = "block";
    document.getElementById("result-owner-address").textContent =
        result.owner_hex;

    // Set sizes
    document.getElementById("result-data-size").textContent =
        `${result.data.length} bytes`;
    document.getElementById("result-total-size").textContent =
        `${result.size} bytes`;

    // Set serialized data
    document.getElementById("serialized-chunk-data").textContent =
        result.serialized_hex;

    // Generate icon
    const iconConfig = new IconConfig(
        200,
        IconShape.Circle,
        GeneratorFunction.Geometric,
        ColorScheme.Vibrant,
    );

    const svgContent = generate_svg_for_address(result.address, iconConfig);
    document.getElementById("created-chunk-icon").innerHTML = svgContent;

    // Show the result container
    document.getElementById("chunk-creation-result").style.display = "block";
}

function analyzeChunk() {
    try {
        // Get the chunk data
        const chunkHexInput = document
            .getElementById("chunk-data-hex")
            .value.trim();
        if (!chunkHexInput) {
            alert("Serialized chunk data is required");
            return;
        }
        const chunkData = hexToBytes(chunkHexInput);

        // Get the expected address
        const addressInput = document
            .getElementById("expected-address")
            .value.trim();
        if (!addressInput) {
            alert("Expected address is required");
            return;
        }
        const expectedAddress = hexToBytes(addressInput);
        if (expectedAddress.length !== 32) {
            alert(
                "Expected address must be exactly 32 bytes (64 hex characters)",
            );
            return;
        }

        // Analyze the chunk
        const result = analyze_chunk(chunkData, expectedAddress);

        // Display the result
        displayAnalysisResult(result);
    } catch (error) {
        alert(`Error analyzing chunk: ${error.message}`);
    }
}

function displayAnalysisResult(result) {
    // Set validity status
    if (result.is_valid) {
        document.getElementById("analysis-valid").style.display = "block";
        document.getElementById("analysis-invalid").style.display = "none";
    } else {
        document.getElementById("analysis-valid").style.display = "none";
        document.getElementById("analysis-invalid").style.display = "block";
        document.getElementById("analysis-error").textContent =
            result.error_message;
    }

    // Set chunk type
    let typeText = "Unknown";
    if (result.chunk_type === 0) {
        typeText = "Content Chunk";
    } else if (result.chunk_type === 1) {
        typeText = "Single Owner Chunk";
    }
    document.getElementById("analysis-chunk-type").textContent = typeText;

    // Set address
    document.getElementById("analysis-chunk-address").textContent =
        result.address_hex;

    // Set ID and owner fields if available
    if (result.has_id) {
        document.getElementById("analysis-id-field").style.display = "block";
        document.getElementById("analysis-chunk-id").textContent =
            result.id_hex;
    } else {
        document.getElementById("analysis-id-field").style.display = "none";
    }

    if (result.has_owner) {
        document.getElementById("analysis-owner-field").style.display = "block";
        document.getElementById("analysis-owner-address").textContent =
            result.owner_hex;
    } else {
        document.getElementById("analysis-owner-field").style.display = "none";
    }

    // Set data size
    document.getElementById("analysis-data-size").textContent =
        `${result.data.length} bytes`;

    // Set total size
    document.getElementById("analysis-total-size").textContent =
        `${result.data.length + (result.has_id ? 32 : 0) + (result.has_signature ? 65 : 0) + 8} bytes`;

    document.getElementById("analysis-chunk-data-hex").textContent =
        "0x" + bytesToHex(result.data);

    try {
        const decoder = new TextDecoder("utf-8", { fatal: true });
        document.getElementById("analysis-chunk-data-text").textContent =
            decoder.decode(result.data);
    } catch {
        document.getElementById("analysis-chunk-data-text").textContent =
            "⚠️ Cannot display as UTF-8: Invalid encoding";
    }

    // Setup the format toggle for analysis chunk data
    setupAnalysisFormatToggle();

    // Generate icon
    const iconConfig = new IconConfig(
        200,
        IconShape.Circle,
        GeneratorFunction.Geometric,
        ColorScheme.Vibrant,
    );

    const svgContent = generate_svg_for_address(result.address, iconConfig);
    document.getElementById("analyzed-chunk-icon").innerHTML = svgContent;

    // Show the result container
    document.getElementById("chunk-analysis-result").style.display = "block";
}

// Start the application when the page loads
window.addEventListener("DOMContentLoaded", initApp);

function setupAnalysisFormatToggle() {
    const toggleOptions = document.querySelectorAll(
        '[data-target="analysis-chunk-data"]',
    );

    toggleOptions.forEach((option) => {
        option.addEventListener("click", () => {
            // Remove active class from all options
            toggleOptions.forEach((opt) => opt.classList.remove("active"));

            // Add active class to clicked option
            option.classList.add("active");

            // Show/hide the appropriate content
            const format = option.getAttribute("data-format");
            if (format === "hex") {
                document.getElementById(
                    "analysis-chunk-data-hex",
                ).style.display = "block";
                document.getElementById(
                    "analysis-chunk-data-text",
                ).style.display = "none";
            } else {
                document.getElementById(
                    "analysis-chunk-data-hex",
                ).style.display = "none";
                document.getElementById(
                    "analysis-chunk-data-text",
                ).style.display = "block";
            }
        });
    });
}
