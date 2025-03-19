// We use ES modules for cleaner dependency management
import init, {
    calculate_bmt_hash,
    benchmark_hash,
    get_library_info,
    generate_svg_icon,
    create_icon_from_hex,
    generate_random_chunk_address,
    IconConfig,
    IconShape,
    GeneratorFunction,
    ColorScheme,
    IconData,
} from "../bmt-wasm-demo.js";

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

        // Ensure size doesn't exceed 4096
        const testSize = Math.min(size, 4096);

        // Update UI to show benchmark is running
        runBenchmarkButton.disabled = true;
        runBenchmarkButton.textContent = "Running...";
        resultPlaceholder.textContent = "Benchmark in progress...";
        benchmarkOutput.innerHTML = "";

        // Add a small delay to allow UI to update
        await new Promise((resolve) => setTimeout(resolve, 50));

        try {
            // Run the benchmark
            const avgTime = benchmark_hash(testSize, iterations);

            // avgTime is in milliseconds per hash operation
            const millisPerOp = avgTime;

            // Convert to operations per second
            const opsPerSecond = 1000 / millisPerOp;

            // Calculate throughput in bytes per second correctly
            const throughput = testSize * opsPerSecond;

            // Format throughput for display
            let throughputDisplay;
            if (throughput < 1024) {
                throughputDisplay = `${throughput.toFixed(2)} B/s`;
            } else if (throughput < 1024 * 1024) {
                throughputDisplay = `${(throughput / 1024).toFixed(2)} KB/s`;
            } else {
                throughputDisplay = `${(throughput / (1024 * 1024)).toFixed(2)} MB/s`;
            }

            // Display results
            resultPlaceholder.textContent = "";
            benchmarkOutput.innerHTML = `
                <div class="benchmark-result-item">
                    <strong>Data Size:</strong> ${formatNumber(testSize)} bytes (${(testSize / 1024).toFixed(2)} KB)
                </div>
                <div class="benchmark-result-item">
                    <strong>Iterations:</strong> ${iterations}
                </div>
                <div class="benchmark-result-item">
                    <strong>Average Time:</strong> ${millisPerOp.toFixed(3)} ms per hash
                </div>
                <div class="benchmark-result-item">
                    <strong>Operations:</strong> ${opsPerSecond.toFixed(2)} hashes/second
                </div>
                <div class="benchmark-result-item">
                    <strong>Throughput:</strong> ${throughputDisplay}
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
}

// Start the application when the page loads
window.addEventListener("DOMContentLoaded", initApp);
