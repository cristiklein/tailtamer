library(ggplot2)
library(tools)

p95 <- function(values) {
  quantile(values, .95)
}

p99 <- function(values) {
  quantile(values, .99)
}

my_plot <- function(input_file_name, x_column, x_label, x_mult=NA, ylim=c(0, 1)) {
  git_dirty <- (length(system('git status --untracked-files=no --porcelain', intern=TRUE)) > 0)
  git_commit <- system('git log -1 --format="%H"', intern=TRUE)

  base_file_name <- file_path_sans_ext(input_file_name)
  
  data <- read.csv(input_file_name)
  if (is.na(x_mult))
    data$x <- as.factor(data[[x_column]])
  else
    data$x <- as.factor(data[[x_column]] * x_mult)

  # Make method name friendlier
  friendly_names  = c("fifo", "cfs", "tie"    , "tt05"    , "tt20"    , "ttP")
  simulator_names = c("fifo", "cfs", "cfs+tie", "tt_0.005", "tt_0.020", "tt+p")
  data$method <- friendly_names[ match(data$method, simulator_names) ]
  
  # TODO: super-inefficient
  data_summary <- NULL
  for (x in levels(data$x)) {
    for (method in unique(data$method)) {
      rt99 = quantile(data$response_time[data$x==x & data$method==method], .99)
      rbind(data_summary, data.frame(x=x, method=method, rt99=rt99)) -> data_summary
    }
  }

  data_improvement <- NULL
  for (x in levels(data$x)) {
    rt99_baseline = min(data_summary$rt99[data_summary$x==x & data_summary$method=='ttP'])
    for (method in unique(data$method)) {
      if (method != 'ttP') {
        rt99 = min(data_summary$rt99[data_summary$x==x & data_summary$method==method])
        worse = (rt99-rt99_baseline)/rt99_baseline*100
        rbind(data_improvement, data.frame(x=x, method=method, rt99=rt99, worse=worse)) -> data_improvement
      }
    }
  }

  # XXX: ugly, increase horizontal spacing in legend
  # http://stackoverflow.com/questions/29953629/add-horizontal-space-between-legend-items
  levels(data$method) <- paste0(levels(data$method), "      ")
  levels(data_improvement$method) <- paste0(levels(data_improvement$method), "      ")

  plot_title = sprintf('Simulation Results (%s%s)', git_commit, if (git_dirty) '+' else '')
  
  dodge <- position_dodge(width = 0.7)
  p <- ggplot(data, aes(x=x, y=response_time, fill=method, shape=method)) +
    scale_shape_manual(values=seq(0, 6)) +
    coord_cartesian(ylim=ylim) +
    geom_violin(position = dodge, show.legend = FALSE) +
    labs(title=plot_title, x=x_label, y = "Response time:\ndistribution & 99th perc. [s]") +
    geom_point(stat = "summary", fun.y = "p99" , position = dodge) +
    guides(shape=guide_legend(nrow=1, title=NULL), fill="none") +
    theme(
      axis.title=element_text(size=10),
      legend.margin = unit(0, "null"),
      legend.key.size = unit(3, "mm"),
      legend.position = 'top',
      panel.margin = unit(0,"null"),
      plot.margin = rep(unit(0,"null"),4)
    )
  
  scale=1 # 1.75 for presentations
  ggsave(paste0(base_file_name, ".pdf"), plot=p, height=2.8*scale, width=5*scale, device=cairo_pdf)
  
  dodge <- position_dodge(width = 0.5)
  p <- ggplot(data_improvement, aes(x=x, y=worse, group=method, shape=method)) +
    scale_shape_manual(values=seq(0, 6)) +
    coord_cartesian(ylim=c(0, 80)) +
    labs(title=plot_title, x=x_label, y = "Increase in 99th perc.\nresponse time [%]") +
    geom_line(mapping=aes(colour=method), show.legend=FALSE, position=dodge) +
    geom_point(position = dodge) +
    guides(shape=guide_legend(nrow=1, title=NULL)) +
    theme(
      axis.title=element_text(size=10),
      legend.key.size = unit(3, "mm"),
      legend.margin = unit(0, "null"),
      legend.position = 'top',
      panel.margin = unit(0,"null"),
      plot.margin = rep(unit(0,"null"),4)
    )
  
  scale=1 # 1.75 for presentations
  ggsave(paste0(base_file_name, "-rel.pdf"), plot=p, height=2.8*scale, width=5*scale, device=cairo_pdf)
}

my_plot('results-ar.csv', 'load', 'Load')
my_plot('results-deg.csv', 'degree', 'Degree')
my_plot('results-mul.csv', 'multiplicity', 'Fan-out')
my_plot('results-var.csv', 'relative_variance', 'Coefficient of variation [%]', x_mult=100)
my_plot('results-ctx.csv', 'context_switch_overhead', 'Context switch overhead [micro-seconds]', x_mult=1000000)
my_plot('results-so.csv', 'num_physical_cpus', 'Number of physical CPUs', ylim=c(0, 0.25))
