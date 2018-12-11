#include "tensorflow/core/framework/dataset.h"
#include "tensorflow/core/framework/common_shape_fns.h"
#include "data/tensorflow/recordio/recordio_reader.h"

using namespace tensorflow;

REGISTER_OP("RecordioDataset")
    .Input("filename: string")
    .Input("offset: int64")
    .Output("handle: variant")
    .SetIsStateful()  
    .SetShapeFn([](shape_inference::InferenceContext* c) {
      shape_inference::ShapeHandle unused;
      // `filename` must be a scalar.
      TF_RETURN_IF_ERROR(c->WithRankAtMost(c->input(0), 1, &unused));
      // `offset` could only be a scalar.
      TF_RETURN_IF_ERROR(c->WithRank(c->input(1), 0, &unused));
      return shape_inference::ScalarShape(c);
    });

class RecordIODatasetOp : public DatasetOpKernel {
 public:
  using DatasetOpKernel::DatasetOpKernel;

  void MakeDataset(OpKernelContext* ctx, DatasetBase** output) override {
    string filename;
    OP_REQUIRES_OK(
        ctx, ParseScalarArgument<string>(ctx, "filename", &filename));
    OP_REQUIRES(ctx, filename.size()> 0,
                errors::InvalidArgument(
                    "invalid argument value for `filename`")); 

    int64 offset = -1;
    OP_REQUIRES_OK(
        ctx, ParseScalarArgument<int64>(ctx, "offset", &offset));
    OP_REQUIRES(ctx, offset>= 0,
                errors::InvalidArgument(
                    "`offset` must be >= 0"));

    *output =
        new Dataset(ctx, filename, offset);
  }

 private:
  class Dataset : public DatasetBase {
   public:
    explicit Dataset(OpKernelContext* ctx, 
                     const string& filename,
                     int64 offset)
        : DatasetBase(DatasetContext(ctx)),
          filename_(filename),
          offset_(offset) {}

    std::unique_ptr<IteratorBase> MakeIteratorInternal(
        const string& prefix) const override {
      return std::unique_ptr<IteratorBase>(
          new Iterator({this, strings::StrCat(prefix, "::RecordIO")}));
    }

    const DataTypeVector& output_dtypes() const override {
      static DataTypeVector* dtypes = new DataTypeVector({DT_STRING});
      return *dtypes;
    }

    const std::vector<PartialTensorShape>& output_shapes() const override {
      static std::vector<PartialTensorShape>* shapes =
          new std::vector<PartialTensorShape>({{}});
      return *shapes;
    }

    string DebugString() const override { return "RecordIODatasetOp::Dataset"; }

   protected:
    Status AsGraphDefInternal(SerializationContext* ctx,
                              DatasetGraphDefBuilder* b,
                              Node** output) const override {
      Node* filename = nullptr;
      TF_RETURN_IF_ERROR(b->AddScalar(filename_, &filename));
      Node* offset = nullptr;
      TF_RETURN_IF_ERROR(b->AddScalar(offset_, &offset));
      TF_RETURN_IF_ERROR(b->AddDataset(
          this, {filename, offset}, output));
      return Status::OK();
    }

   private:
    class Iterator : public DatasetIterator<Dataset> {
     public:
      explicit Iterator(const Params& params)
          : DatasetIterator<Dataset>(params) {}

      Status GetNextInternal(IteratorContext* ctx,
                             std::vector<Tensor>* out_tensors,
                             bool* end_of_sequence) override {
        mutex_lock l(mu_);
        do {
          // We are currently processing a file, so try to read the next record.
          if (reader_) {
            Tensor result_tensor(ctx->allocator({}), DT_STRING, {});
            Status s = reader_->ReadRecord(&result_tensor.scalar<string>()());
            if (s.ok()) {
              out_tensors->emplace_back(std::move(result_tensor));
              *end_of_sequence = false;
              return Status::OK();
            } else if (!errors::IsOutOfRange(s)) {
              return s;
            }

            ResetStreamsLocked();
            *end_of_sequence = true;
            return Status::OK();
          }
          // Initialize the reader.
          TF_RETURN_IF_ERROR(SetupStreamsLocked(ctx->env()));
        } while (true);
      }

     protected:
      Status SaveInternal(IteratorStateWriter* writer) override {
        return Status::OK();
      }

      Status RestoreInternal(IteratorContext* ctx,
                             IteratorStateReader* reader) override {
        mutex_lock l(mu_);
        ResetStreamsLocked();
        TF_RETURN_IF_ERROR(SetupStreamsLocked(ctx->env()));
        return Status::OK();
      }

     private:
      // Sets up reader streams to read from the file at `current_file_index_`.
      Status SetupStreamsLocked(Env* env) EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        string filename = dataset()->filename_;
        TF_RETURN_IF_ERROR(env->NewRandomAccessFile(filename, &file_));

        uint64 file_size = 0;
        TF_RETURN_IF_ERROR(env->GetFileSize(filename, &file_size));

        reader_.reset(
            new io::RecordIOReader(file_.get(), dataset()->offset_));

        return Status::OK();
      }

      // Resets all reader streams.
      void ResetStreamsLocked() EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        reader_.reset();
        file_.reset();
      }

      mutex mu_;

      // `reader_` will borrow the object that `file_` points to, so
      // we must destroy `reader_` before `file_`.
      std::unique_ptr<RandomAccessFile> file_ GUARDED_BY(mu_);
      std::unique_ptr<io::RecordIOReader> reader_ GUARDED_BY(mu_);
    }; // Iterator

    int64 offset_;
    string filename_;
  }; // Dataset
};

REGISTER_KERNEL_BUILDER(Name("RecordioDataset").Device(DEVICE_CPU), RecordIODatasetOp);
