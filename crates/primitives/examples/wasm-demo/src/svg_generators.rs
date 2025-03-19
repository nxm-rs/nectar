use crate::{ColorScheme, IconConfig, IconShape};

// Custom deterministic random number generator based on seed data
pub struct SeedRng {
    state: u64,
}

impl SeedRng {
    pub fn new(seed_data: &[u8]) -> Self {
        // Create a hash of the seed data
        let mut state: u64 = 0;
        for (i, &byte) in seed_data.iter().enumerate() {
            state = state.wrapping_add((byte as u64).wrapping_shl((i % 8) as u32));
            state = state.wrapping_mul(0x5851F42D4C957F2D);
        }
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        // xorshift64* algorithm
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn next_f64(&mut self) -> f64 {
        // Convert to a float between 0 and 1
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) - 1) as f64
    }

    pub fn next_range(&mut self, min: f64, max: f64) -> f64 {
        min + self.next_f64() * (max - min)
    }

    pub fn next_int_range(&mut self, min: i32, max: i32) -> i32 {
        min + (self.next_f64() * (max - min) as f64) as i32
    }
}

// Color palette management
pub fn get_color_palette(scheme: &ColorScheme) -> Vec<&'static str> {
    match scheme {
        ColorScheme::Vibrant => vec![
            "#FF5722", "#2196F3", "#4CAF50", "#FFC107", "#9C27B0", "#3F51B5",
        ],
        ColorScheme::Pastel => vec![
            "#FFD3B6", "#A8E6CE", "#DCEDC2", "#FFD3B5", "#FF8C94", "#91A8D0",
        ],
        ColorScheme::Monochrome => vec![
            "#000000", "#333333", "#666666", "#999999", "#CCCCCC", "#FFFFFF",
        ],
        ColorScheme::Complementary => vec![
            "#2E4172", "#FF6B6B", "#4ECDC4", "#556270", "#C7F464", "#1E2528",
        ],
    }
}

// Utility function to apply a circular clip path if needed
pub fn apply_shape_clipping(svg_content: &str, config: &IconConfig) -> String {
    if let IconShape::Circle = config.shape {
        let size = config.size;
        let radius = size / 2;
        return format!(
            r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">
                <defs>
                    <clipPath id="circleClip">
                        <circle cx="{radius}" cy="{radius}" r="{radius}" />
                    </clipPath>
                </defs>
                <g clip-path="url(#circleClip)">
                    {content}
                </g>
            </svg>"#,
            size = size,
            radius = radius,
            content = svg_content.trim_start_matches(&format!(
                r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">"#,
                size = size
            )).trim_end_matches("</svg>")
        );
    }
    svg_content.to_string()
}

pub fn generate_geometric_icon(seed_data: &[u8], config: &IconConfig) -> String {
    let mut rng = SeedRng::new(seed_data);
    let size = config.size;
    let colors = get_color_palette(&config.color_scheme);

    // Start SVG content
    let mut svg = format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">"#,
        size = size
    );

    // Add background
    let bg_color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
    svg.push_str(&format!(
        r#"<rect width="{size}" height="{size}" fill="{bg_color}" />"#,
        size = size,
        bg_color = bg_color
    ));

    // Number of shapes derived from chunk data
    let num_shapes = 5 + (rng.next_int_range(0, 10));

    for _ in 0..num_shapes {
        let shape_type = rng.next_int_range(0, 3); // 0: rectangle, 1: circle, 2: triangle
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let x = rng.next_range(0.0, size as f64) as i32;
        let y = rng.next_range(0.0, size as f64) as i32;
        let width = 10 + rng.next_range(0.0, size as f64 / 3.0) as i32;
        let height = 10 + rng.next_range(0.0, size as f64 / 3.0) as i32;
        let opacity = 0.3 + rng.next_f64() * 0.7;

        match shape_type {
            0 => {
                // Rectangle
                svg.push_str(&format!(
                    r#"<rect x="{x}" y="{y}" width="{width}" height="{height}" fill="{color}" opacity="{opacity}" />"#,
                    x = x, y = y, width = width, height = height, color = color, opacity = opacity
                ));
            }
            1 => {
                // Circle
                let radius = width.min(height) / 2;
                svg.push_str(&format!(
                    r#"<circle cx="{x}" cy="{y}" r="{radius}" fill="{color}" opacity="{opacity}" />"#,
                    x = x, y = y, radius = radius, color = color, opacity = opacity
                ));
            }
            _ => {
                // Triangle
                let x1 = x;
                let y1 = y;
                let x2 = x + width;
                let y2 = y;
                let x3 = x + width / 2;
                let y3 = y + height;
                svg.push_str(&format!(
                    r#"<polygon points="{x1},{y1} {x2},{y2} {x3},{y3}" fill="{color}" opacity="{opacity}" />"#,
                    x1 = x1, y1 = y1, x2 = x2, y2 = y2, x3 = x3, y3 = y3, color = color, opacity = opacity
                ));
            }
        }
    }

    // Close SVG tag
    svg.push_str("</svg>");

    // Apply shape clipping if needed
    apply_shape_clipping(&svg, config)
}

pub fn generate_abstract_icon(seed_data: &[u8], config: &IconConfig) -> String {
    let mut rng = SeedRng::new(seed_data);
    let size = config.size;
    let colors = get_color_palette(&config.color_scheme);

    // Start SVG content
    let mut svg = format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">"#,
        size = size
    );

    // Add background
    let bg_color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
    svg.push_str(&format!(
        r#"<rect width="{size}" height="{size}" fill="{bg_color}" />"#,
        size = size,
        bg_color = bg_color
    ));

    // Generate paths
    let num_paths = 3 + rng.next_int_range(0, 5);

    for _ in 0..num_paths {
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let stroke = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let stroke_width = 1 + rng.next_int_range(0, 3);
        let opacity = 0.4 + rng.next_f64() * 0.6;

        // Create a bezier curve path
        let mut path = format!(
            "M {} {}",
            rng.next_range(0.0, size as f64) as i32,
            rng.next_range(0.0, size as f64) as i32
        );

        let num_points = 3 + rng.next_int_range(0, 4);

        for _ in 0..num_points {
            let cx1 = rng.next_range(0.0, size as f64) as i32;
            let cy1 = rng.next_range(0.0, size as f64) as i32;
            let cx2 = rng.next_range(0.0, size as f64) as i32;
            let cy2 = rng.next_range(0.0, size as f64) as i32;
            let x = rng.next_range(0.0, size as f64) as i32;
            let y = rng.next_range(0.0, size as f64) as i32;

            path.push_str(&format!(
                " C {cx1} {cy1}, {cx2} {cy2}, {x} {y}",
                cx1 = cx1,
                cy1 = cy1,
                cx2 = cx2,
                cy2 = cy2,
                x = x,
                y = y
            ));
        }

        svg.push_str(&format!(
            r#"<path d="{path}" fill="{color}" stroke="{stroke}" stroke-width="{stroke_width}" opacity="{opacity}" />"#,
            path = path, color = color, stroke = stroke, stroke_width = stroke_width, opacity = opacity
        ));
    }

    // Add some circles for accent
    let num_circles = 4 + rng.next_int_range(0, 8);
    for _ in 0..num_circles {
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let x = rng.next_range(0.0, size as f64) as i32;
        let y = rng.next_range(0.0, size as f64) as i32;
        let radius = 2 + rng.next_range(0.0, 15.0) as i32;
        let opacity = 0.3 + rng.next_f64() * 0.7;

        svg.push_str(&format!(
            r#"<circle cx="{x}" cy="{y}" r="{radius}" fill="{color}" opacity="{opacity}" />"#,
            x = x,
            y = y,
            radius = radius,
            color = color,
            opacity = opacity
        ));
    }

    // Close SVG tag
    svg.push_str("</svg>");

    // Apply shape clipping if needed
    apply_shape_clipping(&svg, config)
}
