library(ggplot2)
library(gridExtra)
library(egg)

# command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) != 2) {
  stop("Rscript efficiency-plot.R <data file> <output file>", call.=FALSE)
}

# import csv data
dataFile <- args[1]
df <- read.table(dataFile, header=TRUE, sep=",", numerals="warn.loss")

pplot <- ggplot(data=df, aes(x=tuples, y=time.sec, group=approach, color=approach, shape = approach))
bp <- pplot + geom_point() +
        facet_wrap(vars(query), scales="fixed") +
        stat_summary(fun.y = mean, geom = "line") +
        labs (x="Number of Tuples in million", y="Execution Time") +
        theme_bw() +
        scale_x_continuous(breaks = c(0, 200000000, 400000000, 600000000), labels = c("0", "200", "400", "600")) +
        scale_y_continuous(breaks = c(0, 5, 10, 15), labels = c("0s", "5s", "10s", "15s")) +
        scale_color_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values=c("#619CFF", "#F8766D", "#00BA38")) + 
        scale_shape_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values = c(16, 17, 15)) +
        coord_cartesian(xlim = c(100000, 600000000), ylim = c(0, 15)) + 
        theme(axis.title.x=element_blank(), strip.background =element_rect(fill="white"), legend.position = "none")
        
sp <- pplot + geom_point() +
        facet_wrap(vars(query), scales="fixed") +
        stat_summary(fun.y = mean, geom = "line") +
        labs (x="Number of Tuples in million", y="Execution Time") +
        theme_bw() +
        scale_x_continuous(breaks = c(0, 25000000, 50000000, 75000000, 100000000), labels = c("0", "25", "50", "75", "100")) +
        scale_y_continuous(breaks = c(0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2), labels = c("0s", "0.2s", "0.4s", "0.6s", "0.8s", "1.0s", "1.2s")) +
        scale_color_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values=c("#619CFF", "#F8766D", "#00BA38")) + 
        scale_shape_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values = c(16, 17, 15)) +
        coord_cartesian(ylim = c(0,1.25), xlim = c(0, 100000000)) +
        theme(strip.text.x = element_blank(), legend.title = element_blank(), legend.position = "bottom")
        
p <- ggarrange(bp, sp, widths = c(5,5), heights = c(2, 2))

# save to output file
ofile <- args[2]
ggsave(ofile, width=4.5, height=4.5, dpi=300, p)
