library(ggplot2)

p95 <- function(values) {
  quantile(values, .95)
}

p99 <- function(values) {
  quantile(values, .99)
}

my_plot <- function(input_file_name, x_column, x_label, x_mult=NA) {
  git_dirty <- (length(system('git status --untracked-files=no --porcelain', intern=TRUE)) > 0)
  git_commit <- system('git log -1 --format="%H"', intern=TRUE)
  
  data <- read.csv(input_file_name)
  data$x <- as.factor(data[[x_column]])

  if (!is.na(x_mult))
    data$x <- data$x * x_mult
  
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
    rt99_baseline = min(data_summary$rt99[data_summary$x==x & data_summary$method=='tt+p'])
    for (method in unique(data$method)) {
      rt99 = min(data_summary$rt99[data_summary$x==x & data_summary$method==method])
      worse = (rt99-rt99_baseline)/rt99_baseline*100
      rbind(data_improvement, data.frame(x=x, method=method, rt99=rt99, worse=worse)) -> data_improvement
    }
  }

  plot_title = sprintf('Simulation Results (%s%s)', git_commit, if (git_dirty) '+' else '')
  
  dodge <- position_dodge(width = 0.7)
  p <- ggplot(data, aes(x=x, y=response_time, fill=method)) +
    scale_y_continuous(limits=c(0,1), expand = c(0, 0)) +
    geom_violin(position = dodge) +
    labs(title=plot_title, x=x_label, y = "Response time [s]") +
    geom_point(stat = "summary", fun.y = "mean", position = dodge, size = 3, show.legend = FALSE) +
    geom_point(stat = "summary", fun.y = "p95" , position = dodge, size = 2, show.legend = FALSE) +
    geom_point(stat = "summary", fun.y = "p99" , position = dodge, show.legend = FALSE) +
    theme(
      legend.margin = unit(0, "null"),
      panel.margin = unit(0,"null"),
      plot.margin = rep(unit(0,"null"),4)
    )
  
  scale=1.75
  ggsave(paste0(input_file_name, ".pdf"), plot=p, height=3*scale, width=4*scale)
  
  p <- ggplot(data_improvement, aes(x=x, y=worse, group=method, colour=method)) +
    labs(title=plot_title, x=x_label, y = "Increase in 99th percentile RT [%]") +
    geom_point() +
    geom_line() +
    theme(
      legend.margin = unit(0, "null"),
      panel.margin = unit(0,"null"),
      plot.margin = rep(unit(0,"null"),4)
    )
  
  scale=1.75
  ggsave(paste0(input_file_name, ".rel.pdf"), plot=p, height=3*scale, width=4*scale)
}

my_plot('results-ar.csv', 'arrival_rate', 'Arrival rate [req/s]')
my_plot('results-deg.csv', 'degree', 'Call-in degree of last layer')
my_plot('results-mul.csv', 'multiplicity', 'Multiplicity of last layer')
my_plot('results-var.csv', 'relative_variance', 'Relative Variance [%]', x_mult=100)
my_plot('results-ctx.csv', 'context_switch_overhead', 'Context Switch Overhead [micro-seconds]', x_mult=1000000)
my_plot('results-tie.csv', 'with_tied_requests', 'With Tied Requests')
