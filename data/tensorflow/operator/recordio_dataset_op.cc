#include "tensorflow/core/framework/dataset.h"
#include "tensorflow/core/framework/common_shape_fns.h"
#include "../recordio/recordio_reader.h"

using namespace tensorflow;

REGISTER_OP("RecordioDataset")
    .Input("filenames: string")
    .Input("start_chunk: int64")
    .Input("chunk_count: int64")
    .Output("handle: variant")
    .SetIsStateful()  
    .SetShapeFn([](shape_inference::InferenceContext* c) {
      shape_inference::ShapeHandle unused;
      // `filenames` must be a scalar or a vector.
      TF_RETURN_IF_ERROR(c->WithRankAtMost(c->input(0), 1, &unused));
      // `start_chunk` could only be a scalar.
      TF_RETURN_IF_ERROR(c->WithRank(c->input(1), 0, &unused));
      // `chunk_count` could only be a scalar.
      TF_RETURN_IF_ERROR(c->WithRank(c->input(2), 0, &unused));
      return shape_inference::ScalarShape(c);
    });

class RecordIODatasetOp : public DatasetOpKernel {
 public:
  using DatasetOpKernel::DatasetOpKernel;

  void MakeDataset(OpKernelContext* ctx, DatasetBase** output) override {
    const Tensor* filenames_tensor;
    OP_REQUIRES_OK(ctx, ctx->input("filenames", &filenames_tensor));
    OP_REQUIRES(
        ctx, filenames_tensor->dims() <= 1,
        errors::InvalidArgument("`filenames` must be a scalar or a vector."));

    std::vector<string> filenames;
    filenames.reserve(filenames_tensor->NumElements());
    for (int i = 0; i < filenames_tensor->NumElements(); ++i) {
      filenames.push_back(filenames_tensor->flat<string>()(i));
    }

    int64 start_chunk = -1;
    OP_REQUIRES_OK(
        ctx, ParseScalarArgument<int64>(ctx, "start_chunk", &start_chunk));
    OP_REQUIRES(ctx, start_chunk>= 0,
                errors::InvalidArgument(
                    "`start_chunk` must be >= 0"));

    int64 chunk_count = -1;
    OP_REQUIRES_OK(
        ctx, ParseScalarArgument<int64>(ctx, "chunk_count", &chunk_count));
    OP_REQUIRES(ctx, chunk_count>= 0,
                errors::InvalidArgument(
                    "`chunk_count` must be >= 0 (0 means read all the chunks)"));

    *output =
        new Dataset(ctx, std::move(filenames), start_chunk, chunk_count);
  }

 private:
  class Dataset : public GraphDatasetBase {
   public:
    explicit Dataset(OpKernelContext* ctx, std::vector<string> filenames,
                     int64 start_chunk, int64 chunk_count)
        : GraphDatasetBase(ctx),
          filenames_(std::move(filenames)),
          start_chunk_(start_chunk),
          chunk_count_(chunk_count) {}

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
    Status AsGraphDefInternal(DatasetGraphDefBuilder* b,
                              Node** output) const override {
      Node* filenames = nullptr;
      TF_RETURN_IF_ERROR(b->AddVector(filenames_, &filenames));
      Node* start_chunk = nullptr;
      TF_RETURN_IF_ERROR(b->AddScalar(start_chunk_, &start_chunk));
      Node* chunk_count = nullptr;
      TF_RETURN_IF_ERROR(b->AddScalar(chunk_count_, &chunk_count));
      TF_RETURN_IF_ERROR(b->AddDataset(
          this, {filenames, start_chunk, chunk_count}, output));
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

            // We have reached the end of the current file, so maybe
            // move on to next file.
            ResetStreamsLocked();
            ++current_file_index_;
          }

          // Iteration ends when there are no more files to process.
          if (current_file_index_ == dataset()->filenames_.size()) {
            *end_of_sequence = true;
            return Status::OK();
          }

          TF_RETURN_IF_ERROR(SetupStreamsLocked(ctx->env()));
        } while (true);
      }

     protected:
      Status SaveInternal(IteratorStateWriter* writer) override {
        mutex_lock l(mu_);
        TF_RETURN_IF_ERROR(writer->WriteScalar(full_name("current_file_index"),
                                               current_file_index_));

        if (reader_) {
          TF_RETURN_IF_ERROR(
              writer->WriteScalar(full_name("offset"), reader_->TellOffset()));
        }
        return Status::OK();
      }

      Status RestoreInternal(IteratorContext* ctx,
                             IteratorStateReader* reader) override {
        mutex_lock l(mu_);
        ResetStreamsLocked();
        int64 current_file_index;
        TF_RETURN_IF_ERROR(reader->ReadScalar(full_name("current_file_index"),
                                              &current_file_index));
        current_file_index_ = size_t(current_file_index);
        if (reader->Contains(full_name("offset"))) {
          int64 offset;
          TF_RETURN_IF_ERROR(reader->ReadScalar(full_name("offset"), &offset));
          TF_RETURN_IF_ERROR(SetupStreamsLocked(ctx->env()));
          TF_RETURN_IF_ERROR(reader_->SeekOffset(offset));
        }
        return Status::OK();
      }

     private:
      // Sets up reader streams to read from the file at `current_file_index_`.
      Status SetupStreamsLocked(Env* env) EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        if (current_file_index_ >= dataset()->filenames_.size()) {
          return errors::InvalidArgument(
              "current_file_index_:", current_file_index_,
              " >= filenames_.size():", dataset()->filenames_.size());
        }

        // Actually move on to next file.
        const string& next_filename =
            dataset()->filenames_[current_file_index_];
        TF_RETURN_IF_ERROR(env->NewRandomAccessFile(next_filename, &file_));

        uint64 file_size = 0;
        TF_RETURN_IF_ERROR(env->GetFileSize(next_filename, &file_size));

        reader_.reset(
            new io::RecordIOReader(file_.get(), file_size, dataset()->start_chunk_, dataset()->chunk_count_));

        return Status::OK();
      }

      // Resets all reader streams.
      void ResetStreamsLocked() EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        reader_.reset();
        file_.reset();
      }

      mutex mu_;
      size_t current_file_index_ GUARDED_BY(mu_) = 0;

      // `reader_` will borrow the object that `file_` points to, so
      // we must destroy `reader_` before `file_`.
      std::unique_ptr<RandomAccessFile> file_ GUARDED_BY(mu_);
      std::unique_ptr<io::RecordIOReader> reader_ GUARDED_BY(mu_);
    }; // Iterator

    int64 start_chunk_;
    int64 chunk_count_;
    const std::vector<string> filenames_;
  }; // Dataset
};

REGISTER_KERNEL_BUILDER(Name("RecordioDataset").Device(DEVICE_CPU), RecordIODatasetOp);
