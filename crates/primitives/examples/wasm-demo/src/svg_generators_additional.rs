use crate::svg_generators::{apply_shape_clipping, get_color_palette, SeedRng};
use crate::{IconConfig, IconShape};

pub fn generate_circular_icon(seed_data: &[u8], config: &IconConfig) -> String {
    let mut rng = SeedRng::new(seed_data);
    let size = config.size;
    let colors = get_color_palette(&config.color_scheme);
    let center_x = size / 2;
    let center_y = size / 2;

    // Start SVG content
    let mut svg = format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">"#,
        size = size
    );

    // Add background
    let bg_color = colors[rng.next_int_range(0, colors.len() as i32) as usize];

    if let IconShape::Circle = config.shape {
        svg.push_str(&format!(
            r#"<circle cx="{center_x}" cy="{center_y}" r="{radius}" fill="{bg_color}" />"#,
            center_x = center_x,
            center_y = center_y,
            radius = size / 2,
            bg_color = bg_color
        ));
    } else {
        svg.push_str(&format!(
            r#"<rect width="{size}" height="{size}" fill="{bg_color}" />"#,
            size = size,
            bg_color = bg_color
        ));
    }

    // Generate concentric rings
    let num_rings = 3 + rng.next_int_range(0, 5);
    let max_radius = size as f64 * 0.45; // Leave some margin

    for i in 0..num_rings {
        let radius = max_radius * (1.0 - i as f64 / num_rings as f64);
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let stroke_width = 1 + rng.next_int_range(0, 5);
        let opacity = 0.3 + rng.next_f64() * 0.7;

        svg.push_str(&format!(
            r#"<circle cx="{center_x}" cy="{center_y}" r="{radius}" fill="none" stroke="{color}" stroke-width="{stroke_width}" opacity="{opacity}" />"#,
            center_x = center_x, center_y = center_y, radius = radius, color = color, stroke_width = stroke_width, opacity = opacity
        ));
    }

    // Add radial lines
    let num_lines = 6 + rng.next_int_range(0, 12);
    let angle_step = 2.0 * std::f64::consts::PI / num_lines as f64;

    for i in 0..num_lines {
        let angle = i as f64 * angle_step;
        let x1 = center_x as f64 + angle.cos() * (max_radius * 0.2);
        let y1 = center_y as f64 + angle.sin() * (max_radius * 0.2);
        let x2 = center_x as f64 + angle.cos() * max_radius;
        let y2 = center_y as f64 + angle.sin() * max_radius;
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let stroke_width = 1 + rng.next_int_range(0, 3);
        let opacity = 0.5 + rng.next_f64() * 0.5;

        svg.push_str(&format!(
            r#"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{stroke_width}" opacity="{opacity}" />"#,
            x1 = x1, y1 = y1, x2 = x2, y2 = y2, color = color, stroke_width = stroke_width, opacity = opacity
        ));
    }

    // Add a few dots
    let num_dots = 5 + rng.next_int_range(0, 10);
    for _ in 0..num_dots {
        let angle = rng.next_f64() * 2.0 * std::f64::consts::PI;
        let distance = rng.next_f64() * max_radius;
        let x = center_x as f64 + angle.cos() * distance;
        let y = center_y as f64 + angle.sin() * distance;
        let radius = 2.0 + rng.next_f64() * 8.0;
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];

        svg.push_str(&format!(
            r#"<circle cx="{x}" cy="{y}" r="{radius}" fill="{color}" />"#,
            x = x,
            y = y,
            radius = radius,
            color = color
        ));
    }

    // Close SVG tag
    svg.push_str("</svg>");

    // Apply shape clipping if needed
    apply_shape_clipping(&svg, config)
}

pub fn generate_pixelated_icon(seed_data: &[u8], config: &IconConfig) -> String {
    let mut rng = SeedRng::new(seed_data);
    let size = config.size;
    let colors = get_color_palette(&config.color_scheme);

    // Determine grid size (4x4 to 12x12)
    let grid_size = 4 + rng.next_int_range(0, 9);
    let cell_size = size as f64 / grid_size as f64;

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

    // Create pixel grid
    for y in 0..grid_size {
        for x in 0..grid_size {
            // Use seed data to determine if this cell should be colored
            let byte_value = rng.next_int_range(0, 256);

            // Only draw if byte value is above threshold (creates patterns with spaces)
            if byte_value > 100 {
                let color_idx = byte_value % colors.len() as i32;
                let color = colors[color_idx as usize];
                let opacity = 0.7 + (byte_value % 20) as f64 / 100.0; // Slight opacity variation

                svg.push_str(&format!(
                    r#"<rect x="{x}" y="{y}" width="{width}" height="{height}" fill="{color}" opacity="{opacity}" />"#,
                    x = x as f64 * cell_size,
                    y = y as f64 * cell_size,
                    width = cell_size,
                    height = cell_size,
                    color = color,
                    opacity = opacity
                ));
            }
        }
    }

    // Close SVG tag
    svg.push_str("</svg>");

    // Apply shape clipping if needed
    apply_shape_clipping(&svg, config)
}

pub fn generate_molecular_icon(seed_data: &[u8], config: &IconConfig) -> String {
    let mut rng = SeedRng::new(seed_data);
    let size = config.size;
    let colors = get_color_palette(&config.color_scheme);
    let center_x = size / 2;
    let center_y = size / 2;

    // Start SVG content
    let mut svg = format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">"#,
        size = size
    );

    // Add background
    let bg_color = colors[rng.next_int_range(0, colors.len() as i32) as usize];

    if let IconShape::Circle = config.shape {
        svg.push_str(&format!(
            r#"<circle cx="{center_x}" cy="{center_y}" r="{radius}" fill="{bg_color}" />"#,
            center_x = center_x,
            center_y = center_y,
            radius = size / 2,
            bg_color = bg_color
        ));
    } else {
        svg.push_str(&format!(
            r#"<rect width="{size}" height="{size}" fill="{bg_color}" />"#,
            size = size,
            bg_color = bg_color
        ));
    }

    // Generate nodes (atoms)
    let num_nodes = 5 + rng.next_int_range(0, 10);
    let mut nodes = Vec::with_capacity(num_nodes as usize);
    let max_radius = size as f64 * 0.4;

    // Central node
    nodes.push((
        center_x as f64,
        center_y as f64,
        8.0 + rng.next_f64() * 10.0,
        colors[rng.next_int_range(0, colors.len() as i32) as usize],
    ));

    // Surrounding nodes
    for _ in 1..num_nodes {
        let angle = rng.next_f64() * 2.0 * std::f64::consts::PI;
        let distance = (0.3 + rng.next_f64() * 0.7) * max_radius;
        let x = center_x as f64 + angle.cos() * distance;
        let y = center_y as f64 + angle.sin() * distance;
        let radius = 4.0 + rng.next_f64() * 12.0;
        let color = colors[rng.next_int_range(0, colors.len() as i32) as usize];

        nodes.push((x, y, radius, color));
    }

    // Generate connections (bonds)
    for i in 1..nodes.len() {
        // Always connect to center node
        let stroke_width = 1 + rng.next_int_range(0, 4);
        let stroke = colors[rng.next_int_range(0, colors.len() as i32) as usize];
        let opacity = 0.6 + rng.next_f64() * 0.4;

        svg.push_str(&format!(
            r#"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{stroke_width}" opacity="{opacity}" />"#,
            x1 = nodes[0].0, y1 = nodes[0].1,
            x2 = nodes[i].0, y2 = nodes[i].1,
            stroke = stroke, stroke_width = stroke_width, opacity = opacity
        ));

        // Sometimes add connections between other nodes
        if rng.next_f64() > 0.7 {
            let j = 1 + rng.next_int_range(0, (nodes.len() - 1) as i32);
            if j as usize != i {
                let stroke_width = 1 + rng.next_int_range(0, 3);
                let stroke = colors[rng.next_int_range(0, colors.len() as i32) as usize];
                let opacity = 0.4 + rng.next_f64() * 0.6;

                svg.push_str(&format!(
                    r#"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{stroke_width}" opacity="{opacity}" />"#,
                    x1 = nodes[i].0, y1 = nodes[i].1,
                    x2 = nodes[j as usize].0, y2 = nodes[j as usize].1,
                    stroke = stroke, stroke_width = stroke_width, opacity = opacity
                ));
            }
        }
    }

    // Draw nodes over connections
    for (x, y, radius, color) in nodes {
        svg.push_str(&format!(
            r#"<circle cx="{x}" cy="{y}" r="{radius}" fill="{color}" />"#,
            x = x,
            y = y,
            radius = radius,
            color = color
        ));
    }

    // Close SVG tag
    svg.push_str("</svg>");

    // Apply shape clipping if needed
    apply_shape_clipping(&svg, config)
}
